package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
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

	ctx, cancel := context.WithTimeout(s.ServerContext, 3*time.Second)
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

	_, err = client.waitForResponseAndBlock(resp)
	if err != nil {
		return s.handleClusterConfigChecksumResponse(client, err)
	}

	// IF we get an ok response we return and continue

	return nil

}

func (s *GBServer) handleClusterConfigChecksumResponse(client *gbClient, respErr error) error {

	handledErr := Errors.HandleError(respErr, func(gbErrors []*Errors.GBError) error {

		// Loop through and check if we have our expected error

		// IF we get a response err of new checksum available then we need to send a digest
		// IF we get a response err of checksum mismatch then we fail early and shutdown
		for _, ge := range gbErrors {
			if ge.Code == Errors.CONFIG_AVAILABLE_CODE {
				// Handle here...
				err := s.sendClusterConfigDigest(client)
				if err != nil {
					return err
				}

				return nil

				// TODO --
				// Still want to return an error, but it's a soft error which we check against to retry until we are clear
				// For now we return nil + fix later

			}

			if ge.Code == Errors.CONFIG_CHECKSUM_FAIL_CODE {

				// Dispatch event here - we don't need an event here as we can just call Shutdown() but the added context and ability to handle helps
				s.DispatchEvent(Event{
					InternalError,
					time.Now().Unix(),
					&ErrorEvent{
						ConnectToSeed,
						Critical,
						respErr,
						"Connect To Seed",
					},
					"",
				})

				return Errors.ChainGBErrorf(Errors.ConnectSeedErr, respErr, "cluster config is not accepted - suggested retrieve config from live cluster and use to bootstrap node")
			}
		}

		return respErr

	})

	return handledErr

}

//TODO Need to streamline and maybe offload some of this function (doing too much?)
// Maybe should just send -> return response -> another function handles the response

func (s *GBServer) sendClusterConfigDigest(client *gbClient) error {

	d, _, err := s.serialiseClusterDigestConfigOnly()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(s.ServerContext, 3*time.Second)
	defer cancel()

	reqID, err := s.acquireReqID()
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConfigDigestErr, err, "")
	}

	pay, err := prepareRequest(d, 1, CFG_RECON, reqID, uint16(0))
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConfigDigestErr, err, "")
	}

	rsp := client.qProtoWithResponse(ctx, reqID, pay, false)

	resp, err := client.waitForResponseAndBlock(rsp)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConfigDigestErr, err, "")
	}
	if resp.msg == nil {
		return Errors.ChainGBErrorf(Errors.ConfigDigestErr, nil, "got nil response")
	}

	// Consider returning the response or calling a handle response function here ---

	cd, err := deserialiseDelta(resp.msg)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConfigDigestErr, err, "")
	}

	if len(cd.delta) > 1 {
		return Errors.ChainGBErrorf(Errors.ConfigDigestErr, err, "length of digest should be 1 got [%d]", len(cd.delta))
	}

	//--------- Update our self

	self := s.GetSelfInfo()

	if delta, ok := cd.delta[cd.sender]; ok {
		for k, v := range delta.keyValues {

			ourD := self.keyValues[k]

			if v.KeyGroup != CONFIG_DKG {
				return Errors.ChainGBErrorf(Errors.ConfigGroupErr, nil, "got [%s]", v.KeyGroup)
			}

			// We don't try to update our view of the sender in our cluster map because we are a new joiner and we have not yet
			// Exchanged self info

			// But we should update our own info to match the config version we've received
			if v.Version <= ourD.Version {
				continue
			}

			self.pm.Lock()
			*ourD = *v
			self.pm.Unlock()

			err := s.updateClusterConfigDeltaAndSelf(v.Key, v)
			if err != nil {
				return Errors.ChainGBErrorf(Errors.ConfigDigestErr, err, "for key [%s]", v.Key)
			}
		}
	} else {
		return Errors.ChainGBErrorf(Errors.ConfigDigestErr, nil, "delta not found for [%s]", s.ServerName)
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
	dl := make([]Delta, 0, 4)

	sizeOfDelta += 1 + len(s.ServerName) + 2 // 1 byte for name length + name + size of delta key-values

	for _, value := range cm.participants[s.ServerName].keyValues {

		if value.Version > version && value.KeyGroup == CONFIG_DKG {
			dl = append(dl, *value)
			size := DELTA_META_SIZE + len(value.KeyGroup) + len(value.Key) + len(value.Value)
			sizeOfDelta += size
		}

	}

	if len(dl) == 0 {
		return cd, 0, fmt.Errorf("no config deltas found of higher version than: %d", version)
	}

	cd[s.ServerName] = dl

	return cd, sizeOfDelta, nil

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

	// Not needed for now
	//respID, err := srv.acquireReqID()
	//if err != nil {
	//	return err
	//}

	cereal, err := srv.serialiseConfigDelta(configDeltas, size)
	if err != nil {
		return fmt.Errorf("sendClusterConfigDelta - serialising configDeltas: %s", err)
	}

	pay, err := prepareRequest(cereal, 1, CFG_RECON_RESP, c.ph.reqID, uint16(0)) // For now, we don't want a response of OK
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.enqueueProto(pay)
	c.mu.Unlock()

	return nil

}

// connectToSeed is called by the server in a go-routine. It blocks on response to wait for the seed server to respond with a signal
// that it has completed INFO exchange. If an error occurs through context, or response error from seed server, then connectToSeed
// will return that error and trigger logic to either retry or exit the process
func (s *GBServer) connectToSeed() error {

	ctx, cancel := context.WithTimeout(s.ServerContext, 3*time.Second)
	defer cancel()

	conn, err := s.dialSeed()
	if err != nil {
		return err
	}

	if conn == nil {
		fmt.Printf("seed not reachable -- should be checking error types here to determine next steps...[TODO]\n")
		// TODO Maybe return a specific error which we can match on and then do a retry
		return nil
	}

	//TODO If we are here - we now need to handle cluster config
	// If we are a seed, we must hash our config - send and compare received hash - if different then we fail early or defer to older seed
	// If we are not a seed, we must send our information and be ready to receive a cluster config

	//----------------
	// Config check to fail early

	client := s.createNodeClient(conn, "tmpClient", true, NODE)
	err = s.sendClusterCgfChecksum(client)
	if err != nil {
		return err
	}

	// Assume response ok if no error

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
		fmt.Printf("[DEBUG] Already connected to node %s, skipping dial\n", node)
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

	s.logger.Info("connecting to node", "name", node, "addr", nodeAddr)

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

//------------------------------------------
// Handling Self Info - Thread safe and for high concurrency

func (s *GBServer) GetSelfInfo() *Participant {
	s.clusterMapLock.RLock()
	defer s.clusterMapLock.RUnlock()
	return s.clusterMap.participants[s.ServerName]
}

// TODO Consider where to put this
func encodeValue(valueType uint8, value any) ([]byte, error) {

	if value == nil {
		return nil, fmt.Errorf("value is nil")
	}

	var buf bytes.Buffer

	switch valueType {
	case D_STRING_TYPE:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		return []byte(v), nil
	case D_BYTE_TYPE:
		v, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte, got %T", value)
		}
		return v, nil
	case D_INT_TYPE:
		v, ok := value.(int)
		if !ok {
			return nil, fmt.Errorf("expected int, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, int64(v)) // normalize size
		return buf.Bytes(), err
	case D_INT8_TYPE:
		v, ok := value.(int8)
		if !ok {
			return nil, fmt.Errorf("expected int8, got %T", value)
		}
		return []byte{byte(v)}, nil
	case D_INT16_TYPE:
		v, ok := value.(int16)
		if !ok {
			return nil, fmt.Errorf("expected int16, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, v)
		return buf.Bytes(), err
	case D_INT32_TYPE:
		v, ok := value.(int32)
		if !ok {
			return nil, fmt.Errorf("expected int32, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, v)
		return buf.Bytes(), err
	case D_INT64_TYPE:
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, v)
		return buf.Bytes(), err
	case D_UINT8_TYPE:
		v, ok := value.(uint8)
		if !ok {
			return nil, fmt.Errorf("expected uint8, got %T", value)
		}
		return []byte{byte(v)}, nil
	case D_UINT16_TYPE:
		v, ok := value.(uint16)
		if !ok {
			return nil, fmt.Errorf("expected uint16, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, v)
		return buf.Bytes(), err
	case D_UINT32_TYPE:
		v, ok := value.(uint32)
		if !ok {
			return nil, fmt.Errorf("expected uint32, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, v)
		return buf.Bytes(), err
	case D_UINT64_TYPE:
		v, ok := value.(uint64)
		if !ok {
			return nil, fmt.Errorf("expected uint64, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, v)
		return buf.Bytes(), err
	case D_FLOAT64_TYPE:
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64, got %T", value)
		}
		err := binary.Write(&buf, binary.BigEndian, v)
		return buf.Bytes(), err
	case D_BOOL_TYPE:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool, got %T", value)
		}
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	default:
		return nil, fmt.Errorf("unsupported valueType: %d", valueType)
	}
}

func (s *GBServer) updateSelfInfo(d *Delta) error {

	self := s.GetSelfInfo()

	self.pm.Lock()
	defer self.pm.Unlock()

	ourD, exists := self.keyValues[MakeDeltaKey(d.KeyGroup, d.Key)]
	if !exists {
		return fmt.Errorf("found no delta for %s-%s", d.KeyGroup, d.Key)
	}

	*ourD = *d

	if d.KeyGroup == CONFIG_DKG {
		if err := s.updateClusterConfigDeltaAndSelf(d.Key, d); err != nil {
			return err
		}
	}

	return nil

}

func (s *GBServer) addDeltaToSelfInfo(d *Delta) error {

	self := s.GetSelfInfo()

	self.pm.Lock()
	defer self.pm.Unlock()

	ourD, exists := self.keyValues[MakeDeltaKey(d.KeyGroup, d.Key)]
	if !exists {
		self.keyValues[MakeDeltaKey(d.KeyGroup, d.Key)] = d
		return nil
	}

	*ourD = *d

	if d.KeyGroup == CONFIG_DKG {
		if err := s.updateClusterConfigDeltaAndSelf(d.Key, d); err != nil {
			return err
		}
	}

	return nil

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
			fmt.Printf("error in onboardNewJoiner: %v\n", err)
		}

		//log.Printf("response from onboardNewJoiner: %v", string(bytes))
		err = c.srv.moveToConnected(c.cid, cd.sender)
		if err != nil {
			fmt.Printf("MoveToConnected failed in process info message: %v\n", err)
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
		return nil, Errors.ChainGBErrorf(Errors.RandomSeedErr, nil, "seed entry list empty")
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

	var candidates []string

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
func generateRandomParticipantIndexesForGossip(
	partArray []string,
	numOfNodeSelection int,
	notToGossip map[string]interface{},
	exclude ...string,
) ([]int, error) {

	if numOfNodeSelection <= 0 {
		return nil, fmt.Errorf("selection count must be >0")
	}
	if len(partArray) == 0 {
		return nil, fmt.Errorf("participant list is empty")
	}

	// Build a quick look-up set of names to skip.
	skip := make(map[string]struct{}, len(notToGossip)+len(exclude))
	for n := range notToGossip {
		skip[n] = struct{}{}
	}
	for _, n := range exclude {
		skip[n] = struct{}{}
	}

	// Collect candidate indexes.
	candidates := make([]int, 0, len(partArray))
	for i, id := range partArray {
		if _, banned := skip[id]; banned {
			continue
		}
		candidates = append(candidates, i)
	}

	if numOfNodeSelection > len(candidates) {
		return nil, fmt.Errorf(
			"cannot select %d nodes: only %d candidates after exclusions",
			numOfNodeSelection, len(candidates))
	}

	// Partial Fisher-Yates shuffle: randomise the first k elements.
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
	case CFG_RECON_RESP:
		c.processCfgReconResp(message)
	case PROBE:
		c.processProbe(message)
	case PROBE_RESP:
		c.processProbeResp(message)
	case PING_CMD:
		c.processPing(message)
	case PONG_CMD:
		c.processPong(message)
	case OK:
		c.processOK(message)
	case OK_RESP:
		c.processOKResp(message)
	case ERR_R:
		c.processErr(message)
	case ERR_RESP:
		c.processErrResp(message)
	default:
		fmt.Printf("unknown command %v\n", c.ph.command)
	}

}

func (c *gbClient) processErr(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		fmt.Printf("response channel closed or context expired for reqID %d\n", reqID)
		return
	}

	msgErr := Errors.BytesToError(message)

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.err <- msgErr: // Non-blocking
		//log.Printf("Error message sent to response channel for reqID %d", c.ph.reqID)
	default:
		fmt.Printf("Warning: response channel full for reqID %d\n", reqID)
	}

}

func (c *gbClient) processErrResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		fmt.Printf("response channel closed or context expired for reqID %d\n", respID)
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	msgErr := Errors.BytesToError(msg)

	select {
	case rsp.err <- msgErr: // Non-blocking
		//log.Printf("Error message sent to response channel for reqID %d", c.ph.reqID)
	default:
		fmt.Printf("Warning: response channel full for respID %d\n", respID)
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
		if cs != srv.originalCfgHash && checksum == srv.originalCfgHash {
			// Here gossip may have changed the cluster config, so we should send our complete cluster config over the network to the receiver
			err := Errors.ChainGBErrorf(
				Errors.ConfigAvailableErr,
				nil,
				"new config available -> got: [%s] -- want: [%s]",
				checksum, cs,
			)

			c.sendErr(reqID, 0, err.Net())

		} else {
			// Now we do a final check against the original hash - if it is different then we send an error which should result in the receiver node shutting down
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

	name, fd, err := deSerialiseDigest(message)
	if err != nil {
		c.sendErr(c.ph.reqID, 0, "deserialise digest failed\r\n")
		return
	}

	err = c.sendClusterConfigDelta(fd, name)
	if err != nil {
		// TODO Need internal event error here
		fmt.Printf("sendClusterConfigDelta failed: %v\n", err)
	}

	return

}

func (c *gbClient) processCfgReconResp(message []byte) {

	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		fmt.Printf("Warning: response channel full for reqID %d\n", reqID)
		return
	}

}

func (c *gbClient) processProbe(message []byte) {

	reqID := c.ph.reqID

	buf := make([]byte, len(message))
	copy(buf, message)
	name := bytes.TrimSuffix(buf, []byte("\r\n"))

	c.srv.logger.Info("got a message", "msg", message, "i am", c.srv.PrettyName())

	client, exists, err := c.srv.getNodeConnFromStore(string(name))
	if err != nil {
		return
	}

	c.srv.logger.Info("client name", "name", client.name)

	if !exists {
		c.srv.logger.Info("client not found", "msg", name)
	}

	probe := []byte("PING\r\n")

	probeReqID, _ := c.srv.acquireReqID()

	pay, err := prepareRequest(probe, 1, PING_CMD, probeReqID, uint16(0))
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

	resp := client.qProtoWithResponse(ctx, probeReqID, pay, false)

	client.waitForResponseAsync(resp, func(payload responsePayload, err error) {

		defer cancel()

		if errors.Is(err, context.DeadlineExceeded) {
			c.sendErr(reqID, 0, "probe timeout")
			return
		}

		if err != nil {
			c.sendErr(reqID, 0, err.Error())
			return
		}

		if payload.msg == nil {
			c.sendErr(reqID, uint16(0), "nil message")
			return
		}

		c.sendOK(reqID)

	})

}

func (c *gbClient) processProbeResp(message []byte) {

}

func (c *gbClient) processPing(message []byte) {

	reqID := c.ph.reqID

	pong := []byte("PONG\r\n")

	c.srv.logger.Info("did we get a ping?", "ping", message)

	pay, err := prepareRequest(pong, 1, PONG_CMD, reqID, uint16(0))
	if err != nil {
		return
	}

	c.mu.Lock()
	c.enqueueProto(pay)
	c.mu.Unlock()

	return

}

func (c *gbClient) processPong(message []byte) {

	reqID := c.ph.reqID
	respID := c.ph.respID

	msg := make([]byte, len(message))
	copy(msg, message)
	pong := bytes.TrimSuffix(msg, []byte("\r\n"))

	//c.srv.logger.Info("did we get a pong?", "ping", message)

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	if rsp == nil {
		return
	}

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: pong}:
	default:
		fmt.Printf("Warning: response channel full for reqID %d\n", reqID)
		return
	}

}

func (c *gbClient) processNewJoinMessage(message []byte) {

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		fmt.Printf("deserialise Delta failed: %v\n", err)
		// Send err response
	}

	err = c.seedSendSelf(tmpC)
	if err != nil {
		fmt.Printf("onboardNewJoiner failed: %v\n", err)
	}

	// We have to do this last because we will end up sending back the nodes own info

	for key, value := range tmpC.delta {

		c.srv.clusterMapLock.RLock()
		cm := c.srv.clusterMap
		c.srv.clusterMapLock.RUnlock()

		if _, exists := cm.participants[key]; !exists {
			err := c.srv.addParticipantFromTmp(key, value)
			if err != nil {
				fmt.Printf("AddParticipantFromTmp failed: %v\n", err)
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
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	if rsp == nil {
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		fmt.Printf("Warning: response channel full for reqID %d\n", reqID)
		return
	}

}

func (c *gbClient) processDiscoveryReq(message []byte) {

	reqID := c.ph.reqID

	// First de-serialise the discovery request
	known, err := deserialiseKnownAddressNodes(message)
	if err != nil {
		fmt.Printf("deserialise KnownAddressNodes failed: %v\n", err)
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
		fmt.Printf("prepareRequest failed: %v\n", err)
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
		fmt.Printf("getResponseChannel failed: %v\n", err)
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
		fmt.Printf("Warning: response channel full for reqID %d\n", reqID)
		return
	}

}

func (c *gbClient) processHandShake(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		fmt.Printf("deserialise Delta failed: %v\n", err)
		// Send err response
	}

	info, err := c.srv.prepareSelfInfoSend(HANDSHAKE_RESP, int(reqID), 0)
	if err != nil {
		fmt.Printf("prepareSelfInfoSend failed: %v\n", err)
	}

	c.mu.Lock()
	c.enqueueProto(info)
	c.mu.Unlock()

	for key, value := range tmpC.delta {
		if _, exists := c.srv.clusterMap.participants[key]; !exists {
			err := c.srv.addParticipantFromTmp(key, value)
			if err != nil {
				fmt.Printf("AddParticipantFromTmp failed: %v\n", err)
				//send err response
			}
		}
		continue
	}

	// Move the tmpClient to connected as it has provided its info which we have now stored
	err = c.srv.moveToConnected(c.cid, tmpC.sender)
	if err != nil {
		fmt.Printf("MoveToConnected failed in process info message: %\n", err)
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
		fmt.Printf("getResponseChannel failed: %v\n", err)
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
		fmt.Printf("Warning: response channel full for reqID %d\n", reqID)
		return
	}

}

func (c *gbClient) processOK(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		fmt.Printf("response channel closed or context expired for reqID %d\n", reqID)
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		fmt.Printf("Warning: response channel full for reqID %d\n", reqID)
		return
	}

}

func (c *gbClient) processOKResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
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
		fmt.Printf("Warning: response channel full for respID %d\n", respID)
		return
	}

}

func (c *gbClient) processGossSyn(message []byte) {

	if c.srv.gossip.gossipPaused {
		return
	}

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	if c.srv.discoveryPhase {
		c.sendErr(reqID, uint16(0), Errors.ConductingDiscoveryErr.Net())
		return
	}

	_, d, err := deSerialiseDigest(message)
	if err != nil {
		fmt.Printf("error serialising digest - %v\n", err)
	}

	//err = c.srv.recordPhi(sender)
	//if err != nil {
	//	fmt.Printf("recordPhi failed: %v\n", err)
	//}

	// Big lesson here (which is why I'm leaving it in):
	// When two nodes attempt to gossip with each other at the same time - I was trying to implement a defer mechanic
	// The node with the highest timestamp wins and makes the other defer by sending an error
	// This would randomly cause gossip round durations to spike and after a while the two nodes would get locked
	// in a context deadline loop
	// Lesson = Let the damn nodes gossip boi

	//deferGossip, err := c.srv.deferGossipRound(senderName)
	//if err != nil {
	//	fmt.Printf("error deferring gossip - %v\n", err)
	//	return
	//}
	//
	//if deferGossip {
	//	c.sendErr(c.ph.reqID, uint16(0), Errors.GossipDeferredErr.Net())
	//	return
	//}

	srv := c.srv

	err = c.sendGossSynAck(srv.ServerName, d)
	if err != nil {
		fmt.Printf("sendGossSynAck failed: %v\n", err)
	}

	return

}

func (c *gbClient) processGossSynAck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	if rsp == nil {
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		fmt.Printf("Warning: response channel full for reqID %d\n", c.ph.reqID)
		return
	}

}

func (c *gbClient) processGossAck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		fmt.Printf("getResponseChannel failed: %v\n", err)
	}

	if rsp == nil {
		fmt.Printf("[WARN] No response channel found for respID %d\n", respID)
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		fmt.Printf("Warning: response channel full for respID %d\n", respID)
		return
	}

}
