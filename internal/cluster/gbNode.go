package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log/slog"
	"math/rand"
	"net"
	"slices"
	"strings"
	"sync"
	"time"
)

/*

gbNode separates server operations from node operations. It is the server as a node on the cluster so all node related operations
are declared and executed here.

All operations which are logically carried out by a node in a cluster must be declared here.

gbNode also processes and dispatches commands received by other nodes in the cluster as well as creating node clients from received connections from other servers

*/

//===================================================================================
// Node Struct which embeds in client
//===================================================================================

type node struct {

	// Info
	tcpAddr   *net.TCPAddr
	direction string
}

//===================================================================================
// Node Connection
//===================================================================================

//-----------------------------------------------
// Creating a node as a client from a connection
//-----------------------------------------------

// createNode is the entry point to reading and writing
// createNode will have a read write loop
// createNode lives inside the node accept loop

// createNodeClient method belongs to the server which receives the connection from the connecting server.
// createNodeClient is passed as a callback function to the acceptConnection method within the AcceptLoop.
func (s *GBServer) createNodeClient(conn net.Conn, name string, clientType int) *gbClient {

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

//===================================================================================
// Handling Cluster Config
//===================================================================================

//-------------------------------
// Cluster Config Checksum

// sendClusterCgfChecksum is used when connecting to another node or mainly seed node during bootstrapping. We send our checksum
// for our cluster config and if it matches with the clusters version we can proceed, if not then we check if we have an original
// configuration and only need to be updated or if we are configured wrong and must fail early
func (s *GBServer) sendClusterCgfChecksum(client *gbClient) error {

	ctx, cancel := context.WithTimeout(s.ServerContext, 2*time.Second)
	defer cancel()

	reqID, err := s.acquireReqID()
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendConfigChecksumErr, err, "")
	}

	cs, err := configChecksum(s.gbClusterConfig)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendConfigChecksumErr, nil, "%s", err.Error())
	}

	buff := make([]byte, len(cs)+2)
	copy(buff, cs)
	copy(buff[len(cs):], CLRF)

	pay, err := prepareRequest(buff, 1, CFG_CHECK, reqID, 0)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendConfigChecksumErr, err, "")
	}

	resp := client.qProtoWithResponse(ctx, reqID, pay, true)

	_, err = client.waitForResponseAndBlock(resp)
	if err != nil {
		return s.handleClusterConfigChecksumResponse(client, err)
	}

	// IF we get an ok response we return and continue

	return nil

}

// handleClusterConfigChecksumResponse takes a response from a config checksum mismatch. If we receive a response of new checksum
// available then we will send a digest in order to receive config updated values. If we have a mismatch on the original cluster
// config then we are configured wrong and must fail early and fast.
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
					return Errors.ChainGBErrorf(Errors.ConfigCheckSumRespErr, err, "")
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

				return Errors.ChainGBErrorf(Errors.ConfigCheckSumRespErr, respErr, "cluster config is not accepted - suggested retrieve config from live cluster and use to bootstrap node")
			}
		}

		return respErr

	})

	return handledErr

}

// sendClusterConfigDigest is responsible for building a digest of our config fields and version to send to a node
// in which we will then receive deltas on the fields we have that are outdated
// once we receive the delta, we deserialise and apply those to our config structure and update our state
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

			// We don't try to update our view of the sender in our cluster map because we are a new joiner and, we have not yet
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

// getConfigDeltasAboveVersion builds a map of config fields which are above the given version in order to build deltas to
// send to a requesting node
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

// sendClusterConfigDelta takes a digest from a requesting node and builds deltas of config fields which are newer in version
// it then sends those to the requesting node so that it may update itself to the current cluster view of the config
func (c *gbClient) sendClusterConfigDelta(fd *fullDigest, sender string) error {

	c.mu.Lock()
	srv := c.srv
	c.mu.Unlock()

	if fd == nil {
		return Errors.ChainGBErrorf(Errors.SendClusterConfigDeltaErr, nil, "received digest is nil")
	}

	entry, ok := (*fd)[sender]
	if !ok || entry == nil {
		return Errors.ChainGBErrorf(Errors.SendClusterConfigDeltaErr, nil, "sender not found in digest map - [%s]", sender)
	}

	// Build method with this in it
	configDeltas, size, err := srv.getConfigDeltasAboveVersion(entry.maxVersion)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendClusterConfigDeltaErr, err, "for sender [%s]", sender)
	}

	cereal, err := srv.serialiseConfigDelta(configDeltas, size)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendClusterConfigDeltaErr, nil, "serialising delta for sender [%s] failed - %s", sender, err.Error())
	}

	pay, err := prepareRequest(cereal, 1, CFG_RECON_RESP, c.ph.reqID, uint16(0)) // For now, we don't want a response of OK
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendClusterConfigDeltaErr, err, "for sender [%s]", sender)
	}

	c.mu.Lock()
	c.enqueueProto(pay)
	c.mu.Unlock()

	return nil

}

//===================================================================================
// Connecting
//===================================================================================

//--------------------------------------------------------
//Connecting To Seed (BootStrapping into the cluster)

// connectToSeed is called by the server in a go-routine. It blocks on response to wait for the seed server to respond with a signal
// that it has completed INFO exchange. If an error occurs through context, or response error from seed server, then connectToSeed
// will return that error and trigger logic to either retry or exit the process and fail early
func (s *GBServer) connectToSeed() error {

	ctx, cancel := context.WithTimeout(s.ServerContext, 3*time.Second)
	defer cancel()

	conn, err := s.dialSeed()
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
	}

	if conn == nil {
		// If we receive a nil conn we have to fail and shutdown as dial seed will have checked all possible configured routes and
		// attempted dials for each - meaning we have no seeds and must first configure and start those for nodes to bootstrap to
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, nil, "seed('s) not reachable - suggest checking seed servers are configured and started first before bootstrapping nodes")
	}

	//----------------
	// Config check to fail early

	client := s.createNodeClient(conn, "tmpClient", NODE)
	err = s.sendClusterCgfChecksum(client)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
	}

	// Assume response ok if no error

	//-----------------
	// Send self info to onboard

	reqID, err := s.acquireReqID()
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
	}

	pay1, err := s.prepareSelfInfoSend(NEW_JOIN, int(reqID), 0)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
	}

	resp := client.qProtoWithResponse(ctx, reqID, pay1, false)

	r, err := client.waitForResponseAndBlock(resp)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
	}

	// If we receive no error we can assume the response was received and continue
	// Sender expects an ok response, so we check if respID is not nil and then send
	if r.respID != 0 {
		client.sendOKResp(r.respID)
	}

	delta, err := deserialiseDelta(r.msg)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, nil, "de-serialise delta for sender [%s] failed - %s", s.ServerName, err.Error())
	}

	// Now we add the delta to our cluster map
	for name, participant := range delta.delta {
		if _, exists := s.clusterMap.participants[name]; !exists {
			err := s.addParticipantFromTmp(name, participant)
			if err != nil {
				return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
			}

			// We also need to the ID to the seed addr list - by adding the uuid of the seed node we have just connected
			// to the corresponding seed addr in the list - we are able to easily map to what seed address we are connected to
			// retrieve the associated uuid and access its connection in the nodeConnStore
			err = s.addIDToSeedAddrList(name, conn.RemoteAddr())
			if err != nil {
				return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
			}

		}
		continue
	}

	// Now we can remove from tmp map and add to client store including connected flag
	err = s.moveToConnected(client.cid, delta.sender)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectSeedErr, err, "")
	}

	client.mu.Lock()
	client.name = delta.sender
	client.mu.Unlock()

	// we call incrementNodeConnCount to safely add to the connection count and also do a check if gossip process needs to be signalled to start/stop based on count
	s.incrementNodeConnCount()

	// Canonical Log Line
	s.logger.Info("Connected to seed",
		slog.String("node", s.PrettyName()),
		slog.String("seed", client.name),
		slog.String("seed addr", client.tcpAddr.String()),
	)

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil

}

//--------------------------------------------------------
//Connecting To A Node

// connectToNodeInMap attempts to make a connection with a node it has its cluster map. The node may have been added through
// gossip with other nodes. We first look in our nodeConnStore to see if we have an active connection, if not, we then
// dial the node and exchange information to form a formal connection
func (s *GBServer) connectToNodeInMap(ctx context.Context, node string) error {

	if conn, exists, err := s.getNodeConnFromStore(node); exists {
		if err != nil {
			return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, err, "")
		}
		//TODO - need to give this more thought as we sometimes encounter instances where a node already has a
		// connection even though we may not have been the one to initiate - this doesn't cause error so for now
		// it's safe to just return and carry on
		// I think it's because we when we receive a connection we also store it but may not have the actual net.Conn
		if conn.gbc != nil {
			return nil
		}
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
		return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, nil, "failed to connect to node [%s] using ip %s -> %s", node, parts[0], err)
	}

	port := parts[1]

	nodeAddr := net.JoinHostPort(ip.String(), port)

	// Dial here
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, nil, "failed to dial node [%s] -> %s", node, err)
	}

	reqID, err := s.acquireReqID()
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, err, "")
	}

	pay1, err := s.prepareSelfInfoSend(HANDSHAKE, int(reqID), 0)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, err, "")
	}

	client := s.createNodeClient(conn, "tmpSeedClient", NODE)

	resp := client.qProtoWithResponse(ctx, reqID, pay1, true)

	r, err := client.waitForResponseAndBlock(resp)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, err, "")
	}
	// If we receive no error we can assume the response was received and continue
	// We do not need to check for respID here because this is a simple request with a response and is not chained

	delta, err := deserialiseDelta(r.msg)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, nil, "deserialise delta failed -> %s", err)
	}

	// Now we add the delta to our cluster map
	for name, part := range delta.delta {
		if _, exists := s.clusterMap.participants[name]; !exists {
			err := s.addParticipantFromTmp(name, part)
			if err != nil {
				return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, err, "")
			}
		}
		continue
	}

	// Now we can remove from tmp map and add to client store including connected flag
	//s.serverLock.Lock()
	err = s.moveToConnected(client.cid, delta.sender)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.ConnectToNodeErr, err, "")
	}

	client.mu.Lock()
	client.name = delta.sender
	client.mu.Unlock()

	// we call incrementNodeConnCount to safely add to the connection count and also do a check if gossip process needs to be signalled to start/stop based on count

	s.incrementNodeConnCount()

	// Canonical log line
	s.logger.Info("Connected to node",
		slog.String("node", node),
		slog.String("addr", client.tcpAddr.String()))

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil

}

//===================================================================================
// Handling Self
//===================================================================================

//--------------------------
// Handling our own info

// GetSelfInfo is a thread safe method to retrieve our own self as a *Participant. We then have
// Access to our view of the cluster map and our own deltas. Changes we make will be to our own self in the
// cluster map which will be gossiped to other nodes
func (s *GBServer) GetSelfInfo() *Participant {
	s.clusterMapLock.RLock()
	defer s.clusterMapLock.RUnlock()
	return s.clusterMap.participants[s.ServerName]
}

// TODO Consider where to put this
// encodeValue is a helper function for when we want to update a delta in our cluster map. We give a valueType
// and a generic value which will allow us to cast to a concrete type and know how to convert to bytes to put
// into a delta value
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
		return []byte{v}, nil
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

// updateSelfInfo is a thread safe method to update one of our own deltas within our self as a *Participant
// it checks if the deltas exists before updating and checks whether the delta is of CONFIG group, if so,
// it will also update the cluster config struct in memory.
// It also compares and updates max version for our *Participant for gossip.
func (s *GBServer) updateSelfInfo(d *Delta) error {

	self := s.GetSelfInfo()

	self.pm.Lock()
	defer self.pm.Unlock()

	ourD, exists := self.keyValues[MakeDeltaKey(d.KeyGroup, d.Key)]
	if !exists {
		return Errors.ChainGBErrorf(Errors.UpdateSelfInfoErr, nil, "found no delta for %s", d.Key)
	}

	*ourD = *d

	if d.KeyGroup == CONFIG_DKG {
		if err := s.updateClusterConfigDeltaAndSelf(d.Key, d); err != nil {
			return Errors.ChainGBErrorf(Errors.UpdateSelfInfoErr, err, "")
		}
	}

	if d.Version > self.maxVersion {
		self.maxVersion = d.Version
	}

	return nil

}

// addDeltaToSelfInfo is similar to updateSelfInfo except we don't return if the delta doesn't exist. Instead,
// we add the delta to our own self *Participant as a new entry in the key-values of our clusterMap
// We also do a CONFIG group check although this may not be needed as we shouldn't be adding new config entries
// once cluster is live (may consider erroring on this in future updates)
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
			return Errors.ChainGBErrorf(Errors.UpdateSelfInfoErr, err, "")
		}
	}

	if d.Version > self.maxVersion {
		self.maxVersion = d.Version
	}

	return nil

}

// Thread safe
// prepareSelfInfoSend gathers the servers deltas into a participant to send over the network. We only send our self info under the assumption that we are a new node
// and have nothing stored in the cluster map. If we do, and StartServer has been called again, or we have reconnected, then the receiving node will detect this by
// running a check on our ServerID + address
func (s *GBServer) prepareSelfInfoSend(command int, reqID, respID int) ([]byte, error) {

	self := s.GetSelfInfo()

	//Need to serialise the tmpCluster
	cereal, err := s.serialiseSelfInfo(self)
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.PrepareSelfInfoErr, nil, "failed to serialise self info -> %s", err.Error())
	}

	pay, err := prepareRequest(cereal, 1, command, uint16(reqID), uint16(respID))
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.PrepareSelfInfoErr, err, "")
	}

	return pay, nil

}

// getKnownAddressNodes is used in the discovery request process. We fetch all known addresses known to us and return as
// a list of strings
func (s *GBServer) getKnownAddressNodes() ([]string, error) {

	// Goss use of locks here?
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
		return nil, Errors.ChainGBErrorf(Errors.KnownAddrErr, nil, "known addr list is nil")
	}
	return known, nil
}

// buildAddrGroupMap used in discoveryRequest is similar to building a delta from a received digest. We return all the known addresses we have
// that the sending node hasn't got in their list which they send to us
func (s *GBServer) buildAddrGroupMap(known []string) (map[string][]string, error) {

	// We go through each participant in the map and build an addr map of advertised addrs

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
// Seed Server Logic
//=======================================================

//---------
//Receiving Node Join

// seedSendSelf is a response to a newJoiner node request. The seed node will send its own information in response to
// receiving a nodes' information. We send our information and wait for an ok response async so as not to block any hot paths
func (c *gbClient) seedSendSelf(cd *clusterDelta) error {

	s := c.srv

	respID, err := s.acquireReqID()
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SeedSendSelfErr, err, "")
	}

	self, err := s.prepareSelfInfoSend(SELF_INFO, int(c.ph.reqID), int(respID))
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SeedSendSelfErr, err, "")
	}

	ctx, cancel := context.WithTimeout(s.ServerContext, 1*time.Second)

	resp := c.qProtoWithResponse(ctx, respID, self, false)

	// If we fail here we hit our deadline timeout
	c.waitForResponseAsync(resp, func(bytes responsePayload, err error) {

		defer cancel()
		if err != nil {
			s.logger.Info("Sending self info as seed failed", slog.String("err", err.Error()))
			return
		}

		if bytes.msg != nil {
			err = c.srv.moveToConnected(c.cid, cd.sender)
			if err != nil {
				return
			}

			c.srv.incrementNodeConnCount()
		} else {
			s.logger.Info("sending seed self info failed - msg = nil")
			return
		}

	})

	return nil

}

//=======================================================
// Retrieving Connections or Nodes
//=======================================================

// getRandomSeedToDial retrieves a random seed and is used by nodes to bootstrap into the cluster. It is called by dialSeed
// and ensures that all seed addresses are exhausted before returning and failing causing the node to terminate early
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

//----------------------
// Retrieve a seed conn

// Possibly redundant as the only method using this is discovery request - can use getRandomSeedToDial instead?

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

//------------------------
// Random node selector

// Lock should be held on entry
// generateRandomParticipantIndexesForGossip is used mainly in a gossip round to randomly select n amount of nodes to gossip
// with. We do this by building an active list of candidates, shuffling them and appending to a slice consisting of their indexes
// which we use to index into the participantArray.
// we can also exclude extra nodes by adding their names - this is useful when generating a list of viable nodes to indirectly probe
// when we suspect the current node we may be gossiping with
func generateRandomParticipantIndexesForGossip(
	partArray []string,
	numOfNodeSelection int,
	notToGossip *sync.Map,
	exclude ...string,
) ([]int, error) {

	if numOfNodeSelection <= 0 {
		return nil, Errors.ChainGBErrorf(Errors.RandomIndexesErr, nil, "number of node selection must be positive")
	}
	if len(partArray) == 0 {
		return nil, Errors.ChainGBErrorf(Errors.RandomIndexesErr, nil, "empty participant list")
	}

	// Build a quick look-up set of names to skip.
	skip := make(map[string]struct{})
	// Add all keys from sync.Map
	notToGossip.Range(func(key, _ any) bool {
		if s, ok := key.(string); ok {
			skip[s] = struct{}{}
		}
		return true
	})
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
		return nil,
			Errors.ChainGBErrorf(Errors.RandomIndexesErr, nil, "cannot select %d nodes: only %d candidates active after exclusions", numOfNodeSelection, len(candidates))
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
	case PING_CMD:
		c.processPing()
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
		//Unknown command - drop
		// Should we clear buffers and message to get rid of it?
		return
	}

}

// processErr handles an error sent by a responder to our request
func (c *gbClient) processErr(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		// Need to do anything here?
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msgErr := Errors.BytesToError(message)

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.err <- msgErr: // Non-blocking
	default:
		// channel full - drop
		return
	}

}

// processErrResp processes an error which has been sent by the original requester in answer to our response
func (c *gbClient) processErrResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	msgErr := Errors.BytesToError(msg)

	select {
	case rsp.err <- msgErr: // Non-blocking
	default:
		return
	}

}

// processCfgCheck takes the checksum send by the requester (in this case it should be a new node joining the cluster) and compares
// it against the checksums we have. First against our current checksum and, if different, against the original checksum
// this node was configured with
func (c *gbClient) processCfgCheck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	// Message should be 64 bytes long for a checksum + 2 for CLRF

	if len(message) != 66 {
		c.sendErr(reqID, Errors.ChainGBErrorf(Errors.InvalidChecksumErr, nil, "got %d - should be 66", len(message)).Net())
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
		// TODO We will want an error event here as this is an internal system error? For now just log it
		c.srv.logger.Error("Error checking config checksum", slog.String("err", err.Error()))
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

			c.sendErr(reqID, err.Net())

		} else {
			// Now we do a final check against the original hash - if it is different then we send an error which should result in the receiver node shutting down
			err := Errors.ChainGBErrorf(
				Errors.ConfigChecksumFailErr,
				nil, // no inner cause here
				"checksum should be: [%s] or: [%s] -- got: [%s]",
				cs, srv.originalCfgHash, checksum,
			)

			c.sendErr(reqID, err.Net())

		}

	} else {
		c.sendOK(reqID)
	}
}

// processCfgRecon processes a cluster config digest from a requesting node and produces a delta including only the
// config fields which the requesting node needs which they have outdated versions of
func (c *gbClient) processCfgRecon(message []byte) {

	name, fd, err := deSerialiseDigest(message)
	if err != nil {
		c.sendErr(c.ph.reqID, "deserialise digest failed\r\n")
		return
	}

	err = c.sendClusterConfigDelta(fd, name)
	if err != nil {
		// TODO Need internal event error here?
		c.srv.logger.Error("Error sending cluster configuration delta", slog.String("err", err.Error()))
	}

	return

}

// processCfgReconResp is a response from a node in which we have just sent a config delta to. We will either receive
// an error telling us we have the wrong config leading us to shut down or, we have updated config fields which we
// can apply to our config state.
func (c *gbClient) processCfgReconResp(message []byte) {

	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		return
	}

}

// processProbe processes a request by a node to probe a target node which is being suspected as unresponsive. The requesting
// node sends us the targets name, and we PING the target and wait async for a response via timeout
func (c *gbClient) processProbe(message []byte) {

	start := time.Now()
	s := c.srv

	reqID := c.ph.reqID

	buf := make([]byte, len(message))
	copy(buf, message)
	name := bytes.TrimSuffix(buf, []byte("\r\n"))

	client, exists, err := c.srv.getNodeConnFromStore(string(name))
	if err != nil {
		return
	}

	if !exists {
		s.logger.Warn("received a probe request",
			slog.String("requester", c.name),
			slog.String("status", "failed - target does not exist in our conn store"),
			slog.Duration("duration", time.Since(start)))

		return
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
			c.sendErr(reqID, Errors.ProbeFailedErr.Net())

			s.logger.Warn("received a probe request",
				slog.String("requester", c.name),
				slog.String("target", client.name),
				slog.String("status", "failed"),
				slog.String("err", err.Error()),
				slog.Duration("duration", time.Since(start)))

			return
		}

		if err != nil {
			c.sendErr(reqID, Errors.ProbeFailedErr.Net())

			s.logger.Warn("received a probe request",
				slog.String("requester", c.name),
				slog.String("target", client.name),
				slog.String("status", "failed"),
				slog.String("err", err.Error()),
				slog.Duration("duration", time.Since(start)))

			return
		}

		if payload.msg == nil {
			c.sendErr(reqID, Errors.ProbeFailedErr.Net())

			s.logger.Warn("received a probe request",
				slog.String("requester", c.name),
				slog.String("target", client.name),
				slog.String("status", "failed - nil response"),
				slog.Duration("duration", time.Since(start)))

			return
		}

		c.sendOK(reqID)

		s.logger.Warn("received a probe request",
			slog.String("requester", c.name),
			slog.String("target", client.name),
			slog.String("status", "success"),
			slog.Duration("duration", time.Since(start)))

	})

}

// processPing is a probe by another node where we may have been suspected. We answer PONG to tell the probe we are alive
func (c *gbClient) processPing() {

	reqID := c.ph.reqID

	pong := []byte("PONG\r\n")

	pay, err := prepareRequest(pong, 1, PONG_CMD, reqID, uint16(0))
	if err != nil {
		return
	}

	c.mu.Lock()
	c.enqueueProto(pay)
	c.mu.Unlock()

	return

}

// processPong is the suspected nodes response in the probe request we will have made and are waiting async for via response channel
func (c *gbClient) processPong(message []byte) {

	reqID := c.ph.reqID
	respID := c.ph.respID

	msg := make([]byte, len(message))
	copy(msg, message)
	pong := bytes.TrimSuffix(msg, []byte("\r\n"))

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: pong}:
	default:
		return
	}

}

// processNewJoinMessage processes the information send by a joining node to the cluster, this usually means we are
// a seed node here bootstrapping the requesting node. We deserialise the deltas and send our own self information.
func (c *gbClient) processNewJoinMessage(message []byte) {

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		c.srv.logger.Error("deserialise delta failed in process new join message", "err", err.Error())
	}

	err = c.seedSendSelf(tmpC)
	if err != nil {
		// TODO May be want an internal error event here?
		c.srv.logger.Error("Error in processing new join message", slog.String("err", err.Error()))
	}

	// We have to do this last because we will end up sending back the nodes own info if we send self after adding
	// to our map

	for key, value := range tmpC.delta {

		c.srv.clusterMapLock.RLock()
		cm := c.srv.clusterMap
		c.srv.clusterMapLock.RUnlock()

		if _, exists := cm.participants[key]; !exists {
			err := c.srv.addParticipantFromTmp(key, value)
			if err != nil {
				c.srv.logger.Error("Error in processing new join message", slog.String("err", err.Error()))
				// Do we also want an internal error or send an err resp?
				return
			}
		}
		continue
	}

	return

}

// processSelfInfo takes a response from a seed server which has sent its information to us. The information is then
// sent to the awaiting response payload channels
func (c *gbClient) processSelfInfo(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
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
		c.sendErr(c.ph.reqID, Errors.EmptyAddrMapNetworkErr.Net())
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

// processHandShake processes the request from a node which has dialled us for the first time during gossip. It takes the
// received handshake and deserialises the delta before sending our own information back. It then adds the deltas and
// participant to our cluster map
func (c *gbClient) processHandShake(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		c.srv.logger.Error("deserialise delta failed in process handshake", "err", err.Error())
	}

	info, err := c.srv.prepareSelfInfoSend(HANDSHAKE_RESP, int(reqID), 0)
	if err != nil {
		c.srv.logger.Error("Error in process handshake", slog.String("err", err.Error()))
	}

	c.mu.Lock()
	c.enqueueProto(info)
	c.mu.Unlock()

	for key, value := range tmpC.delta {
		if _, exists := c.srv.clusterMap.participants[key]; !exists {
			err := c.srv.addParticipantFromTmp(key, value)
			if err != nil {
				c.srv.logger.Error("Error in process handshake", slog.String("err", err.Error()))
				//send err response
			}
		}
		continue
	}

	// Move the tmpClient to connected as it has provided its info which we have now stored
	err = c.srv.moveToConnected(c.cid, tmpC.sender)
	if err != nil {
		c.srv.logger.Error("Error in process handshake", slog.String("err", err.Error()))
	}

	c.srv.incrementNodeConnCount()

	return

}

// processHandShakeResp sends a response from a handshake request to the waiting response handler. The response should
// consist of the responding servers information.
func (c *gbClient) processHandShakeResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		return
	}

}

// processOK takes an ok response sent over the network and sends it to the response channel waiting for a response
func (c *gbClient) processOK(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		return
	}

}

// processOKResp sends an ok response received from an original requester in a request-response chain where we have responded
// and want a confirmation. The response is sent to the awaiting response channel
func (c *gbClient) processOKResp(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
		return
	default:
		return
	}

}

// processGossSyn takes a digest send from a requesting node, deserialises and then send and an acknowledgement
func (c *gbClient) processGossSyn(message []byte) {

	if c.srv.gossip.gossipPaused {
		return
	}

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID

	if c.srv.discoveryPhase {
		c.sendErr(reqID, Errors.ConductingDiscoveryErr.Net())
		return
	}

	_, d, err := deSerialiseDigest(message)
	if err != nil {
		c.srv.logger.Error("deserialise delta failed in process goss_syn", "err", err.Error())
	}

	srv := c.srv

	err = c.sendGossSynAck(srv.ServerName, d)
	if err != nil {
		return
	}

	return

}

// processGossSynAck receives a responding digest from a goss_syn we have just made. The digest may also be accompanied by
// a delta. We send the parsed packet to the awaiting response channel.
func (c *gbClient) processGossSynAck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(reqID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		return
	}

}

// processGossAck takes the final acknowledgement in the gossip chain cycle and sends to awaiting response handlers
func (c *gbClient) processGossAck(message []byte) {

	// Copy IDs early to avoid race or mutation
	reqID := c.ph.reqID
	respID := c.ph.respID

	rsp, err := c.getResponseChannel(respID)
	if err != nil {
		c.srv.logger.Error("Error getting response channel", slog.String("err", err.Error()))
		return
	}

	if rsp == nil || rsp.ctx.Err() != nil {
		c.srv.logger.Error("Response error - got nil response or context err", slog.String("err", rsp.ctx.Err().Error()))
		return
	}

	msg := make([]byte, len(message))
	copy(msg, message)

	select {
	case rsp.ch <- responsePayload{reqID: reqID, respID: respID, msg: msg}:
	default:
		return
	}

}
