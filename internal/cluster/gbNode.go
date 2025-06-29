package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
	"math/rand"
	"net"
	"slices"
	"strings"
	"time"
)

//===================================================================================
// Node Struct which embeds in client
//===================================================================================

const (
	INITIATED = "Initiated"
	RECEIVED  = "Received"
)

type node struct {

	// Info
	tcpAddr   *net.TCPAddr
	direction string
}

//===================================================================================
// Node Connection
//===================================================================================

//-------------------------------
// Creating a node as a client from a connection
//-------------------------------

// createNode is the entry point to reading and writing
// createNode will have a read write loop
// createNode lives inside the node accept loop

// createNodeClient method belongs to the server which receives the connection from the connecting server.
// createNodeClient is passed as a callback function to the acceptConnection method within the AcceptLoop.
func (s *GBServer) createNodeClient(conn net.Conn, name string, initiated bool, clientType int) *gbClient {

	now := time.Now()
	clientName := fmt.Sprintf("%s_%d", name, now.Unix())

	client := &gbClient{
		name:    clientName,
		created: now,
		srv:     s,
		gbc:     conn,
		cType:   clientType,
	}

	client.mu.Lock()
	client.initClient()
	client.mu.Unlock()

	// We temporarily store the client connection here until we have completed the info exchange and received a response.
	s.tmpConnStore.Store(client.cid, client)

	//May want to update some node connection metrics which will probably need a write lock from here
	// Start time, reference count etc

	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.PrettyName(), fmt.Sprintf("read loop for %s", name), func() {
		client.readLoop()
	})

	//Write loop -
	s.startGoRoutine(s.PrettyName(), fmt.Sprintf("write loop for %s", name), func() {
		client.writeLoop()
	})

	return client

}

//-------------------------------
// Connecting to seed server
//-------------------------------

//-------------------------------
// Cluster Config Checksum

func (s *GBServer) sendClusterCgfChecksum(client *gbClient) error {

	ctx, cancel := context.WithTimeout(s.ServerContext, 1*time.Second)
	defer cancel()

	reqID, err := s.acquireReqID()
	if err != nil {
		return fmt.Errorf("acquire request ID: %v", err)
	}

	cs, err := configChecksum(s.gbClusterConfig)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	buff := make([]byte, len(cs)+2)
	copy(buff, cs)
	copy(buff[len(cs):], CLRF)

	pay, err := prepareRequest(buff, 1, CFG_CHECK, reqID, 0)
	if err != nil {
		return fmt.Errorf("prepare request: %v", err)
	}

	resp := client.qProtoWithResponse(ctx, reqID, pay, true)

	r, err := client.waitForResponseAndBlock(resp)
	if err != nil {
		handledErr := Errors.HandleError(err, func(gbErrors []*Errors.GBError) error {

			// Loop through and check if we have our expected error
			for _, ge := range gbErrors {
				if errors.Is(ge, Errors.ConfigChecksumFailErr) {
					// Handle here...
					// TODO Now we send a config delta digest and wait for a response
				}
			}

			// Do we handle inside callback?

			return err

		})

		return handledErr

	}

	// IF we get a response err of new checksum available then we need to send a digest
	// IF we get a response err of checksum mismatch then we fail early and shutdown

	// IF we get an ok response we return and continue

	log.Printf("config response = %s", string(r.msg))

	return nil

}

// connectToSeed is called by the server in a go-routine. It blocks on response to wait for the seed server to respond with a signal
// that it has completed INFO exchange. If an error occurs through context, or response error from seed server, then connectToSeed
// will return that error and trigger logic to either retry or exit the process
func (s *GBServer) connectToSeed() error {

	ctx, cancel := context.WithTimeout(s.ServerContext, 1*time.Second)
	defer cancel()

	conn, err := s.dialSeed()
	if err != nil {
		return err
	}

	if conn == nil {
		log.Printf("seed not reachable -- should be checking error types here to determine next steps...[TODO]")
		// TODO Maybe return a specific error which we can match on and then do a retry
		return nil
	}

	//TODO If we are here - we now need to handle cluster config
	// If we are a seed, we must hash our config - send and compare received hash - if different then we fail early or defer to older seed
	// If we are not a seed, we must send our information and be ready to receive a cluster config

	//----------------
	// Config check to fail early

	client := s.createNodeClient(conn, "tmpClient", true, NODE)

	// Assume response ok if no error
	log.Printf("checking cluster config...")
	err = s.sendClusterCgfChecksum(client)
	if err != nil {
		return err
	}

	//-----------------
	// Send self info to onboard

	reqID, err := s.acquireReqID()
	if err != nil {
		return fmt.Errorf("connect to seed - acquire request ID: %s", err)
	}

	pay1, err := s.prepareSelfInfoSend(NEW_JOIN, int(reqID), 0)
	if err != nil {
		return err
	}

	resp := client.qProtoWithResponse(ctx, reqID, pay1, false)

	r, err := client.waitForResponseAndBlock(resp)
	if err != nil {
		// TODO We need to check the response err if we receive - error code which we may be able to ignore or do something with or a system error which we need to return
		// TODO Also check the r.err channel
		return fmt.Errorf("error waiting for self info response - %v", err)
	}

	// If we receive no error we can assume the response was received and continue
	// ---> We should check if there is a respID and then sendOKResp
	if r.respID != 0 {
		client.sendOKResp(r.respID)
	}

	delta, err := deserialiseDelta(r.msg)
	if err != nil {
		return fmt.Errorf("connect to seed - deserialising data: %s", err)
	}

	// Now we add the delta to our cluster map
	for name, participant := range delta.delta {
		if _, exists := s.clusterMap.participants[name]; !exists {
			err := s.addParticipantFromTmp(name, participant)
			if err != nil {
				return fmt.Errorf("connect to seed - adding participant from tmp: %s", err)
			}

			// We also need to the ID to the seed addr list
			log.Printf("adding id -- %s to seed addr list with addr --> %s", name, conn.RemoteAddr().String())
			err = s.addIDToSeedAddrList(name, conn.RemoteAddr())
			if err != nil {
				return fmt.Errorf("connect to seed - adding seed id to addr list - %v", err)
			}

		}
		continue
	}

	// Now we can remove from tmp map and add to client store including connected flag
	err = s.moveToConnected(client.cid, delta.sender)
	if err != nil {
		return err
	}

	client.mu.Lock()
	client.name = delta.sender
	client.mu.Unlock()

	// we call incrementNodeConnCount to safely add to the connection count and also do a check if gossip process needs to be signalled to start/stop based on count
	s.incrementNodeConnCount()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil

}

// TODO Re-visit as we need to do address checks and also defer or reach out to other nodes if we have no addr

func (s *GBServer) connectToNodeInMap(ctx context.Context, node string) error {

	if _, exists, err := s.getNodeConnFromStore(node); exists {
		if err != nil {
			return err
		}
		log.Printf("[DEBUG] Already connected to node %s, skipping dial", node)
		return nil
	}

	s.clusterMapLock.RLock()
	participant := s.clusterMap.participants[node]

	addrKey := MakeDeltaKey(ADDR_DKG, _ADDRESS_)

	var addr []byte

	if kv, exists := participant.keyValues[addrKey]; !exists {
		s.clusterMapLock.RUnlock()
		return fmt.Errorf("no address key in map")
	} else {
		addr = kv.Value
	}
	s.clusterMapLock.RUnlock()

	parts := strings.Split(string(addr), ":")

	ip, err := net.ResolveIPAddr("ip", parts[0])
	if err != nil {
		return fmt.Errorf("connectToNodeInMap resolving ip address: %s", err)
	}

	port := parts[1]

	nodeAddr := net.JoinHostPort(ip.String(), port)

	log.Printf("connecting to node %s at %s", node, nodeAddr)

	// Dial here
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return fmt.Errorf("connectToNodeInMap - net dial: %s", err)
	}

	reqID, err := s.acquireReqID()
	if err != nil {
		return fmt.Errorf("connectToNodeInMap - acquire request ID: %s", err)
	}

	pay1, err := s.prepareSelfInfoSend(HANDSHAKE, int(reqID), 0)
	if err != nil {
		return err
	}

	client := s.createNodeClient(conn, "tmpSeedClient", true, NODE)

	resp := client.qProtoWithResponse(ctx, reqID, pay1, true)

	r, err := client.waitForResponseAndBlock(resp)
	if err != nil {
		// TODO We need to check the response err if we receive - error code which we may be able to ignore or do something with or a system error which we need to return
		return fmt.Errorf("connectToNodeInMap - wait for response: %s", err)
	}
	// If we receive no error we can assume the response was received and continue
	// We do not need to check for respID here in r because this is a simple request with a response and is not chained

	delta, err := deserialiseDelta(r.msg)
	if err != nil {
		return fmt.Errorf("connectToNodeInMap - deserialising data: %s", err)
	}

	// Now we add the delta to our cluster map
	for name, part := range delta.delta {
		if _, exists := s.clusterMap.participants[name]; !exists {
			err := s.addParticipantFromTmp(name, part)
			if err != nil {
				return fmt.Errorf("connect to seed - adding participant from tmp: %s", err)
			}
		}
		continue
	}

	// Now we can remove from tmp map and add to client store including connected flag
	//s.serverLock.Lock()
	err = s.moveToConnected(client.cid, delta.sender)
	if err != nil {
		return fmt.Errorf("connectToNodeInMap - moving connection to connected: %s", err)
	}
	//s.serverLock.Unlock()

	client.mu.Lock()
	client.name = delta.sender
	client.mu.Unlock()

	// we call incrementNodeConnCount to safely add to the connection count and also do a check if gossip process needs to be signalled to start/stop based on count

	s.incrementNodeConnCount()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil

}

//=====================================================================
// Config Reconciliation for initial bootstrap
//=====================================================================

func (s *GBServer) getConfigDeltasAboveVersion(version int64) (map[string][]Delta, int, error) {

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	sizeOfDelta := 0

	cd := make(map[string][]Delta)

	for key, value := range cm.participants[s.ServerName].keyValues {

		sizeOfDelta += 1 + len(s.ServerName) + 2 // 1 byte for name length + name + size of delta key-values

		if value.Version > version {
			cd[key] = append(cd[key], *value)
			sizeOfDelta += DELTA_META_SIZE + len(value.KeyGroup) + len(value.Key) + len(value.Value)
		}

	}

	if len(cd) == 0 {
		return cd, 0, fmt.Errorf("no config deltas found of higher version than: %d", version)
	}

	return cd, sizeOfDelta, nil

}

func (s *GBServer) reconcileClusterConfig() error {

	//digestToSend, _, err := s.serialiseClusterDigestConfigOnly()
	//if err != nil {
	//	return err
	//}

	return nil

}

func (c *gbClient) sendClusterConfigDelta(fd *fullDigest, sender string) error {

	c.mu.Lock()
	srv := c.srv
	c.mu.Unlock()

	if fd == nil {
		return fmt.Errorf("fulld digest is nil")
	}

	entry, ok := (*fd)[sender]
	if !ok || entry == nil {
		return fmt.Errorf("%s not found in full difest map", sender)
	}

	// Build method with this in it
	configDeltas, size, err := srv.getConfigDeltasAboveVersion(entry.maxVersion)
	if err != nil {
		return err
	}

	cereal, err := srv.serialiseACKDelta(configDeltas, size)
	if err != nil {
		return fmt.Errorf("sendClusterConfigDelta - serialising configDeltas: %s", err)
	}

	pay, err := prepareRequest(cereal, 1, CFG_RECON, c.ph.respID, uint16(0))
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.enqueueProto(pay)
	c.mu.Unlock()

	return nil

}

//------------------------------------------
// Handling Self Info - Thread safe and for high concurrency

func (s *GBServer) GetSelfInfo() *Participant {
	s.clusterMapLock.RLock()
	defer s.clusterMapLock.RUnlock()
	return s.clusterMap.participants[s.ServerName]
}

func (s *GBServer) updateSelfInfo(timeOfUpdate int64, updateFunc func(participant *Participant, timeOfUpdate int64) error) {

	if s.gbNodeConfig.Internal.DisableInternalGossipSystemUpdate {
		log.Printf("internal systems gossip update is off")
	}

	self := s.GetSelfInfo()

	err := updateFunc(self, timeOfUpdate)
	if err != nil {
		log.Printf("error %v", err)
	}

	s.clusterMapLock.Lock()
	s.clusterMap.participants[s.ServerName].maxVersion = timeOfUpdate
	s.clusterMapLock.Unlock()

}

// Assume no lock held coming
func (s *GBServer) updateParticipant(node *Participant, timeOfUpdate int64, update func(node *Participant, timeOfUpdate int64) error) {

	err := update(node, timeOfUpdate)
	if err != nil {
		log.Printf("error %v", err)
	}

}

// Thread safe
// prepareSelfInfoSend gathers the servers deltas into a participant to send over the network. We only send our self info under the assumption that we are a new node
// and have nothing stored in the cluster map. If we do, and StartServer has been called again, or we have reconnected, then the receiving node will detect this by
// running a check on our ServerID + address
func (s *GBServer) prepareSelfInfoSend(command int, reqID, respID int) ([]byte, error) {

	//s.clusterMapLock.RLock()
	self := s.GetSelfInfo()

	//Need to serialise the tmpCluster
	cereal, err := s.serialiseSelfInfo(self)
	if err != nil {
		return nil, fmt.Errorf("prepareSelfInfoSend - serialising self info: %s", err)
	}

	pay, err := prepareRequest(cereal, 1, command, uint16(reqID), uint16(respID))
	if err != nil {
		return nil, err
	}

	return pay, nil

}

func (s *GBServer) getKnownAddressNodes() ([]string, error) {
	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	known := make([]string, 0)

	for _, p := range cm.participants {

		addrKey := MakeDeltaKey(ADDR_DKG, _ADDRESS_)

		if _, exists := p.keyValues[addrKey]; exists {
			known = append(known, p.name)
		}
	}
	if len(known) == 0 {
		return nil, Errors.KnownInternalErrors[Errors.KNOWN_ADDR_CODE]
	}
	return known, nil
}

func (s *GBServer) buildAddrGroupMap(known []string) (map[string][]string, error) {

	// We go through each participant in the map and build an addr map of advertised addrs
	// We may want to only include a certain network type...?

	var sizeEstimate int

	sizeEstimate += NODE_HEADER_SIZE_V1

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	addrMap := make(map[string][]string)

	for _, n := range cm.participants {

		name := n.name

		if slices.Contains(known, name) {
			// We skip if the node already has the address
			continue
		}

		if uint16(sizeEstimate+len(name)) > DEFAULT_MAX_DISCOVERY_SIZE {
			return addrMap, nil
		}

		sizeEstimate += len(name)

		addrMap[name] = make([]string, 0)

		for key, value := range n.keyValues {
			if value.KeyGroup == ADDR_DKG {
				log.Printf("tcpKey = %v", key)

				if uint16(sizeEstimate+len(key)) > DEFAULT_MAX_DISCOVERY_SIZE {
					return addrMap, nil
				} else {
					addrMap[name] = append(addrMap[name], value.Key)
				}
			} else {
				continue
			}
		}
	}

	if len(addrMap) == 0 {
		return nil, nil
	}

	return addrMap, nil
}

//=======================================================
// Seed Server
//=======================================================

//---------
//Receiving Node Join

func (c *gbClient) seedSendSelf(cd *clusterDelta) error {

	s := c.srv

	respID, err := s.acquireReqID()
	if err != nil {
		return fmt.Errorf("seedSendSelf: %w", err)
	}

	self, err := s.prepareSelfInfoSend(SELF_INFO, int(c.ph.reqID), int(respID))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(s.ServerContext, 2*time.Second)
	//defer cancel()

	resp := c.qProtoWithResponse(ctx, respID, self, false)

	c.waitForResponseAsync(resp, func(bytes responsePayload, err error) {

		defer cancel()
		if err != nil {
			log.Printf("error in onboardNewJoiner: %v", err)
		}

		log.Printf("resp ===== in new on board = %s", bytes.msg)

		//log.Printf("response from onboardNewJoiner: %v", string(bytes))
		err = c.srv.moveToConnected(c.cid, cd.sender)
		if err != nil {
			log.Printf("MoveToConnected failed in process info message: %v", err)
		}

		c.srv.incrementNodeConnCount()

	})

	return nil

}

//=======================================================
// Retrieving Connections or Nodes
//=======================================================

//-----------------
// Retrieve a random seed to dial

func (s *GBServer) getRandomSeedToDial() (*seedEntry, error) {

	var candidates []*seedEntry

	if s.isSeed {
		// Exclude self from list
		for _, seed := range s.seedAddr {
			if seed.resolved.String() != s.advertiseAddress.String() {
				candidates = append(candidates, seed)
			}
		}
	} else {
		for _, seed := range s.seedAddr {
			candidates = append(candidates, seed)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available seed candidates to connect to")
	}

	return candidates[rand.Intn(len(candidates))], nil

}

//-----------------
// Retrieve a seed conn

func (s *GBServer) retrieveASeedConn(random bool) (*gbClient, error) {
	// First try the original seed node connection (usually second in participantArray)
	if conn, ok := s.nodeConnStore.Load(s.clusterMap.participantArray[1]); ok && !random {
		return conn.(*gbClient), nil
	}

	log.Printf("No connection to original seed. Falling back to other seeds...")

	var candidates []string

	log.Printf("name = %s", s.String())

	if s.isSeed {
		// Exclude self from candidate list
		for _, seed := range s.seedAddr {
			if seed.nodeID != s.String() {
				candidates = append(candidates, seed.nodeID)
			}
		}
	} else {
		// Include all seeds (we're a seed ourselves)
		for _, seed := range s.seedAddr {
			candidates = append(candidates, seed.nodeID)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available seed candidates to connect to")
	}

	// Pick a seed candidate (random or first)
	var selected string
	if random {
		selected = candidates[rand.Intn(len(candidates))]
	} else {
		selected = candidates[0]
	}

	conn, ok := s.nodeConnStore.Load(selected)
	if !ok {
		return nil, fmt.Errorf("no connection found for selected seed: %s", selected)
	}

	return conn.(*gbClient), nil
}

//-----------------
// Random node selector

// Lock should be held on entry
func generateRandomParticipantIndexesForGossip(partArray []string, numOfNodeSelection int, excludeID string) ([]int, error) {
	partLenArray := len(partArray)
	if partLenArray <= 0 || numOfNodeSelection <= 0 {
		return nil, fmt.Errorf("invalid participant array or selection count")
	}

	// Build list of candidate indexes, excluding self
	candidates := make([]int, 0, partLenArray-1)
	for i, id := range partArray {
		if id != excludeID {
			candidates = append(candidates, i)
		}
	}

	if numOfNodeSelection > len(candidates) {
		return nil, fmt.Errorf("not enough participants to select %d (excluding self)", numOfNodeSelection)
	}

	// Partial Fisher-Yates shuffle to pick numOfNodeSelection indexes
	for i := 0; i < numOfNodeSelection; i++ {
		j := i + rand.Intn(len(candidates)-i)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}

	return candidates[:numOfNodeSelection], nil
}

//===================================================================================
// Parser Message Processing - Dispatched from processMessage() called in parser
//===================================================================================

//---------------------------
// Node Handlers

func (c *gbClient) dispatchNodeCommands(message []byte) {

	//GOSS_SYN
	//GOSS_SYN_ACK
	//GOSS_ACK
	//TEST
	switch c.ph.command {
	case NEW_JOIN:
		c.processNewJoinMessage(message)
	case SELF_INFO:
		c.processSelfInfo(message)
	case DISCOVERY_REQ:
		c.processDiscoveryReq(message)
	case DISCOVERY_RES:
		c.processDiscoveryRes(message)
	case HANDSHAKE:
		c.processHandShake(message)
	case HANDSHAKE_RESP:
		c.processHandShakeResp(message)
	case GOSS_SYN:
		c.processGossSyn(message)
	case GOSS_SYN_ACK:
		c.processGossSynAck(message)
	case GOSS_ACK:
		c.processGossAck(message)
	case CFG_CHECK:
		c.processCfgCheck(message)
	case CFG_RECON:
		c.processCfgRecon(message)
	case OK:
		c.processOK(message)
	case OK_RESP:
		c.processOKResp(message)
	case ERR_R:
		c.processErr(message)
	case ERR_RESP:
		c.processErrResp(message)
	default:
		log.Printf("unknown command %v", c.ph.command)
	}

}

func (c *gbClient) processErr(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		log.Printf("response channel closed or context expired for reqID %d", reqID)
		return
	}

	msgErr := Errors.BytesToError(message)

	log.Printf("msg err ==== %s", msgErr)

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.err <- msgErr: // Non-blocking
		//log.Printf("Error message sent to response channel for reqID %d", c.ph.reqID)
	default:
		log.Printf("Warning: response channel full for reqID %d", reqID)
	}

}

func (c *gbClient) processErrResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		log.Printf("response channel closed or context expired for reqID %d", respID)
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	msgErr := Errors.BytesToError(msg)

	select {
	case rsp.err <- msgErr: // Non-blocking
		//log.Printf("Error message sent to response channel for reqID %d", c.ph.reqID)
	default:
		log.Printf("Warning: response channel full for respID %d", respID)
	}

}

func (c *gbClient) processCfgCheck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	// Message should be 64 bytes long for a checksum + 2 for CLRF

	if len(message) != 66 {
		c.sendErr(reqID, 0, "invalid configuration check message length\r\n") // TODO Change to GBError
	}

	// We need to compare against our config checksums
	// TODO Continue::

	c.srv.serverLock.Lock()
	srv := c.srv
	cfg := srv.gbClusterConfig
	c.srv.serverLock.Unlock()

	checksum := string(message[:64])

	// First check against our current hash
	cs, err := configChecksum(cfg)
	if err != nil {
		// TODO We will want an error event here as this is an internal system error
		return
	}

	if checksum != cs {

		// If checksum received is different then we must check if our cs is different from our original config checksum on server start
		if cs != srv.originalCfgHash {
			// Now we do a final check against the original hash - if it is different then we send an error which should result in the receiver node shutting down

		} else {
			// Here gossip may have changed the cluster config, so we should send our complete cluster config over the network to the receiver
			err := Errors.ChainGBErrorf(
				Errors.ConfigChecksumFailErr,
				nil, // no inner cause here
				"checksum should be: [%s] or: [%s] -- got: [%s]",
				cs, srv.originalCfgHash, checksum,
			)

			c.sendErr(reqID, 0, err.Net())

		}

	} else {
		c.sendOK(reqID)
	}
}

func (c *gbClient) processCfgRecon(message []byte) {

	//name, fd, err := deSerialiseDigest(message)
	//if err != nil {
	//	c.sendErr(c.ph.reqID, 0, err.Error())
	//	return
	//}

	return

}

func (c *gbClient) processNewJoinMessage(message []byte) {

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		log.Printf("deserialise Delta failed: %v", err)
		// Send err response
	}

	// TODO Do we need to do a seed check on ourselves first?
	err = c.seedSendSelf(tmpC)
	if err != nil {
		log.Printf("onboardNewJoiner failed: %v", err)
	}

	// We have to do this last because we will end up sending back the nodes own info

	for key, value := range tmpC.delta {

		c.srv.clusterMapLock.RLock()
		cm := c.srv.clusterMap
		c.srv.clusterMapLock.RUnlock()

		if _, exists := cm.participants[key]; !exists {
			err := c.srv.addParticipantFromTmp(key, value)
			if err != nil {
				log.Printf("AddParticipantFromTmp failed: %v", err)
				//send err response
			}
		}
		continue
	}

	return

}

func (c *gbClient) processSelfInfo(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil {
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		log.Printf("Warning: response channel full for reqID %d", reqID)
		return
	}

}

func (c *gbClient) processDiscoveryReq(message []byte) {

	reqID := c.ph.reqID

	// First de-serialise the discovery request
	known, err := deserialiseKnownAddressNodes(message)
	if err != nil {
		log.Printf("deserialise KnownAddressNodes failed: %v", err)
		return
	}

	cereal, err := c.discoveryResponse(known)

	// TODO Use handle error function here
	if err != nil && cereal == nil {
		// TODO Need to check what the error is first
		c.sendErr(c.ph.reqID, uint16(0), Errors.EmptyAddrMapNetworkErr.Net())
		return

	}

	// Echo back the reqID
	pay, err := prepareRequest(cereal, 1, DISCOVERY_RES, reqID, uint16(0))
	if err != nil {
		log.Printf("prepareRequest failed: %v", err)
	}

	c.mu.Lock()
	c.enqueueProto(pay)
	c.mu.Unlock()

	return

}

func (c *gbClient) processDiscoveryRes(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil {
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		log.Printf("Warning: response channel full for reqID %d", reqID)
		return
	}

}

func (c *gbClient) processHandShake(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		log.Printf("deserialise Delta failed: %v", err)
		// Send err response
	}

	info, err := c.srv.prepareSelfInfoSend(HANDSHAKE_RESP, int(reqID), 0)
	if err != nil {
		log.Printf("prepareSelfInfoSend failed: %v", err)
	}

	c.mu.Lock()
	c.enqueueProto(info)
	c.mu.Unlock()

	for key, value := range tmpC.delta {
		if _, exists := c.srv.clusterMap.participants[key]; !exists {
			err := c.srv.addParticipantFromTmp(key, value)
			if err != nil {
				log.Printf("AddParticipantFromTmp failed: %v", err)
				//send err response
			}
		}
		continue
	}

	// Move the tmpClient to connected as it has provided its info which we have now stored
	err = c.srv.moveToConnected(c.cid, tmpC.sender)
	if err != nil {
		log.Printf("MoveToConnected failed in process info message: %v", err)
	}

	// TODO Monitor the server lock here and be mindful
	c.srv.incrementNodeConnCount()

	return

}

func (c *gbClient) processHandShakeResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil {
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		log.Printf("Warning: response channel full for reqID %d", reqID)
		return
	}

}

func (c *gbClient) processOK(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		log.Printf("response channel closed or context expired for reqID %d", reqID)
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		log.Printf("Warning: response channel full for reqID %d", reqID)
		return
	}

}

func (c *gbClient) processOKResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil {
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		log.Printf("Warning: response channel full for respID %d", respID)
		return
	}

}

func (c *gbClient) processGossSyn(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	if c.srv.discoveryPhase {
		c.sendErr(reqID, uint16(0), Errors.ConductingDiscoveryErr.Net())
		return
	}

	sender, d, err := deSerialiseDigest(message)
	if err != nil {
		log.Printf("error serialising digest - %v", err)
	}

	senderName := sender

	err = c.srv.recordPhi(senderName)
	if err != nil {
		log.Printf("recordPhi failed: %v", err)
	}

	//Does the sending node need to defer?
	//If it does - then we must construct an error response, so it can exit out of it's round
	deferGossip, err := c.srv.deferGossipRound(senderName)
	if err != nil {
		log.Printf("error deferring gossip - %v", err)
		return
	}

	if deferGossip {
		c.sendErr(c.ph.reqID, uint16(0), Errors.GossipDeferredErr.Net())
		return
	}

	srv := c.srv

	err = c.sendGossSynAck(srv.ServerName, d)
	if err != nil {
		log.Printf("sendGossSynAck failed: %v", err)
	}

	return

}

func (c *gbClient) processGossSynAck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil {
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		log.Printf("Warning: response channel full for reqID %d", c.ph.reqID)
		return
	}

}

func (c *gbClient) processGossAck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		log.Printf("getResponseChannel failed: %v", err)
	}

	if rsp == nil {
		log.Printf("[WARN] No response channel found for respID %d", respID)
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		log.Printf("Warning: response channel full for respID %d", respID)
		return
	}

}
