package src

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
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

	gossipFlags gossipFlags
}

//===================================================================================
// Node Connection
//===================================================================================

type gossipFlags int

// Flags
const (
	GOSS_SYN_SENT = 1 << iota
	GOSS_SYN_REC
	GOSS_SYN_ACK_SENT
	GOSS_SYN_ACK_REC
	GOSS_ACK_SENT
	GOSS_ACK_REC
)

//goland:noinspection GoMixedReceiverTypes
func (gf *gossipFlags) set(g gossipFlags) {
	*gf |= g
}

//goland:noinspection GoMixedReceiverTypes
func (gf *gossipFlags) clear(g gossipFlags) {
	*gf &= ^g
}

//goland:noinspection GoMixedReceiverTypes
func (gf gossipFlags) isSet(g gossipFlags) bool {
	return gf&g != 0
}

//goland:noinspection GoMixedReceiverTypes
func (gf *gossipFlags) setIfNotSet(g gossipFlags) bool {
	if *gf&g == 0 {
		*gf |= g
		return true
	}
	return false
}

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

	s.serverLock.Lock()
	// We temporarily store the client connection here until we have completed the info exchange and recieved a response.
	s.tmpClientStore[client.cid] = client
	//log.Printf("adding client to tmp store %v\n", s.tmpClientStore[client.cid])
	s.serverLock.Unlock()

	//May want to update some node connection  metrics which will probably need a write lock from here
	// Node count + connection map

	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.ServerName, fmt.Sprintf("read loop for %s", name), func() {
		client.readLoop()
	})

	//Write loop -
	s.startGoRoutine(s.ServerName, fmt.Sprintf("write loop for %s", name), func() {
		client.writeLoop()
	})

	// Only log if the connection was initiated by this server (to avoid duplicate logs)
	if initiated {
		client.directionType = INITIATED
		//log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.name, clientType, conn.LocalAddr())

	} else {
		client.directionType = RECEIVED
		//log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.name, clientType, conn.RemoteAddr())

	}

	return client

}

//-------------------------------
// Connecting to seed server
//-------------------------------

// TODO Think about reconnection here - do we want to handle that in this method? Or have a reconnectToSeed() method?

// connectToSeed is called by the server in a go-routine. It blocks on response to wait for the seed server to respond with a signal
// that it has completed INFO exchange. If an error occurs through context, or response error from seed server, then connectToSeed
// will return that error and trigger logic to either retry or exit the process
func (s *GBServer) connectToSeed() error {

	ctx, cancel := context.WithTimeout(s.serverContext, 1*time.Second)
	defer cancel()

	addr := net.JoinHostPort(s.gbConfig.SeedServers[0].SeedIP, s.gbConfig.SeedServers[0].SeedPort)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}

	respID, err := s.acquireReqID()
	if err != nil {
		return err
	}

	pay1, err := s.prepareSelfInfoSend(INFO, int(respID), 0)
	if err != nil {
		return err
	}

	client := s.createNodeClient(conn, "tmpSeedClient", true, NODE)

	rspChan, errChan := client.qProtoWithResponse(ctx, respID, pay1, false, true)
	if err != nil {
		return fmt.Errorf("%s response error in connect to seed: %s", s.ServerName, err)
	}

	// Wait for either a response or an error
	var rsp []byte

	select {
	case rsp = <-rspChan:
		log.Printf("Received response: %s", string(rsp))

	case err = <-errChan:
		log.Printf("Error received: %v", err)
		return err

	case <-ctx.Done():
		log.Printf("Timeout or cancellation while waiting for response")
		return ctx.Err()
	}
	// If we receive no error we can assume the response was received and continue

	delta, err := deserialiseDelta(rsp)
	if err != nil {
		return err
	}

	// Now we add the delta to our cluster map
	for name, participant := range delta.delta {

		err := s.addParticipantFromTmp(name, participant)
		if err != nil {
			return err
		}
	}

	// Now we can remove from tmp map and add to client store including connected flag
	//s.serverLock.Lock()
	err = s.moveToConnected(client.cid, delta.sender)
	if err != nil {
		return err
	}
	//s.serverLock.Unlock()

	client.mu.Lock()
	client.name = delta.sender
	client.mu.Unlock()

	// we call incrementNodeConnCount to safely add to the connection count and also do a check if gossip process needs to be signalled to start/stop based on count

	s.incrementNodeConnCount()

	select {
	case <-ctx.Done():
		log.Println("connect to seed cancelled because of context")
		return ctx.Err()
	default:
	}

	return nil

}

func (s *GBServer) connectToNodeInMap(ctx context.Context, node string) error {

	// Must clean up resources if we get timeout or error response (example - seq ID)

	s.clusterMapLock.RLock()
	participant := s.clusterMap.participants[node]

	addr := participant.keyValues[_ADDRESS_].value
	s.clusterMapLock.RUnlock()

	parts := strings.Split(string(addr), ":")

	ip, err := net.ResolveIPAddr("ip", parts[0])
	if err != nil {
		return fmt.Errorf("error in connectToNodeInMap while resolving ip address: %s", err)
	}

	port := parts[1]

	nodeAddr := net.JoinHostPort(ip.String(), port)

	log.Printf("connecting to node %s at %s", node, nodeAddr)

	// Dial here
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}

	info, err := s.prepareSelfInfoSend(HANDSHAKE, -1, 0)
	if err != nil {
		return err
	}

	client := s.createNodeClient(conn, "tmpSeedClient", true, NODE)

	// Flush is false because we have started a new client routine and write loop + signals may not be up yet
	rspChan, errChan := client.qProtoWithResponse(ctx, 0, info, false, true)
	if err != nil {
		return fmt.Errorf("%s response error in connect to seed: %s", s.ServerName, err)
	}

	// Wait for either a response or an error
	var rsp []byte

	select {
	case rsp = <-rspChan:
		log.Printf("Received response: %s", string(rsp))

	case err = <-errChan:
		log.Printf("Error received: %v", err)
		return err

	case <-ctx.Done():
		log.Printf("Timeout or cancellation while waiting for response")
		return ctx.Err()
	}
	// If we receive no error we can assume the response was received and continue

	// First de-serialise the delta and then we can move sender to store

	delta, err := deserialiseDelta(rsp)

	// From here we then move the temp client to node store and increment the node count
	err = s.moveToConnected(client.cid, delta.sender)
	if err != nil {
		return err
	}

	client.mu.Lock()
	client.name = delta.sender
	client.mu.Unlock()

	//we call incrementNodeConnCount to safely add to the connection count and also do a check if gossip process needs to be signalled to start/stop based on count

	s.incrementNodeConnCount()

	return nil

}

// Thread safe
// prepareSelfInfoSend gathers the servers deltas into a participant to send over the network. We only send our self info under the assumption that we are a new node
// and have nothing stored in the cluster map. If we do, and StartServer has been called again, or we have reconnected, then the receiving node will detect this by
// running a check on our ServerID + address
func (s *GBServer) prepareSelfInfoSend(command int, reqID, respID int) ([]byte, error) {

	s.clusterMapLock.RLock()

	//Need to serialise the tmpCluster
	cereal, err := s.serialiseSelfInfo()
	if err != nil {
		return nil, err
	}

	log.Printf("seq %v", reqID)

	s.clusterMapLock.RUnlock()

	// Construct header
	header := constructNodeHeader(1, uint8(command), uint16(reqID), uint16(respID), uint16(len(cereal)), NODE_HEADER_SIZE_V1, 0, 0)
	// Create packet
	packet := &nodePacket{
		header,
		cereal,
	}
	pay1, err := packet.serialize()
	if err != nil {
		return nil, err
	}

	return pay1, nil

}

//=======================================================
// Seed Server
//=======================================================

//---------
//Receiving Node Join

// Compute how many nodes we need to send and if we need to split it up
// We will know how many crucial internal state deltas we'll need to send so the total size can be estimated?
// From there gossip will up to the new node, but it must stay in joining state for a few rounds depending on the cluster map size
// for routing and so that it is not queried and so it's not engaging in application logic such as writing to files etc. just yet

func (c *gbClient) onboardNewJoiner() error {

	s := c.srv

	//TODO we will use a hybrid bootstrap approach by selecting a random number of participants to download to the new joining node
	// This will be based on how many nodes are in the map

	if len(s.clusterMap.participantQ) > 100 {
		log.Printf("lots of participants - may need more efficient snapshot transfer")
		// In this case we would send an INFO_ACK to tell the joining node that more info will come
		// The joining node can then reach out to other seed servers or send another request to this seed server
		// With the digest of what it has been given so far to complete it's joining
		// A mini bootstrapped gossip between joiner and seeds

		// TODO If cluster too large - will need to stream data and use metadata to track progress
	}

	s.clusterMapLock.Lock()

	msg, err := s.serialiseClusterDelta()
	if err != nil {
		return err
	}

	s.clusterMapLock.Unlock()

	// TODO -- We should be able to specify a responder ID here now which will chain the request-response cycle

	//respID, err := s.acquireReqID()
	//if err != nil {
	//	return err
	//}

	hdr := constructNodeHeader(1, INFO_ALL, c.ph.reqID, 1, uint16(len(msg)), NODE_HEADER_SIZE_V1, 0, 0)

	packet := &nodePacket{
		hdr,
		msg,
	}

	pay1, err := packet.serialize()
	if err != nil {
		return err
	}

	//TODO Wait for response is causing a deadlock - need to look at if we want to chain request-response cycle
	// if queue with response is used

	//ctx, cancel := context.WithTimeout(s.serverContext, 1*time.Second)
	//defer cancel()
	//
	//resp, err := c.qProtoWithResponse(ctx, 1, pay1, true, true)
	//if err != nil {
	//	return err
	//}
	//
	//log.Printf("onboard response = %v", string(resp))
	//
	//if resp != nil {
	//	return nil
	//}

	c.mu.Lock()
	c.qProto(pay1, true)
	c.mu.Unlock()

	return fmt.Errorf("no response in onboardNewJoiner")

}

func (c *gbClient) processArg(arg []byte) error {
	// Assuming the first 3 bytes represent the command and the next bytes represent msgLength

	// TODO may need a arg dispatcher or different arg processors

	if len(arg) >= 4 {
		c.ph.version = arg[0]
		c.ph.command = arg[1]
		c.ph.reqID = binary.BigEndian.Uint16(arg[2:4])
		c.ph.respID = binary.BigEndian.Uint16(arg[4:6])
		// Extract the last 4 bytes
		msgLengthBytes := arg[6:8]
		// Convert those 4 bytes to uint32 (BigEndian)
		c.ph.msgLength = int(binary.BigEndian.Uint16(msgLengthBytes))

		// Log the result to verify
		//log.Printf("Extracted msgLength: %d\n", c.ph.msgLength)
	} else {
		return fmt.Errorf("argument does not have enough bytes to extract msgLength")
	}

	c.argBuf = arg
	log.Printf("arg == %v", c.argBuf)

	return nil
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
	case INFO:
		c.processInfoMessage(message)
	case INFO_ALL:
		c.processInfoAll(message)
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
	case OK:
		c.processOK(message)
	case ERR_RESP:
		c.processErrResp(message)
	default:
		log.Printf("unknown command %v", c.ph.command)
	}

}

func (c *gbClient) processErrResp(message []byte) {

	c.rh.rm.Lock()
	responseChan, exists := c.rh.resp[int(c.ph.reqID)]
	c.rh.rm.Unlock()

	msg := make([]byte, len(message))
	copy(msg, message)

	if exists {

		// We just send the message and allow the caller to specify what they do with it
		responseChan.err <- fmt.Errorf("%s", msg)

	} else {
		log.Printf("no response channel found")
		// Else handle as normal command
	}

}

func (c *gbClient) processInfoAll(message []byte) {

	c.rh.rm.Lock()
	requestChan, exists := c.rh.resp[int(c.ph.reqID)]
	c.rh.rm.Unlock()

	msg := make([]byte, len(message))
	copy(msg, message)

	if exists {

		// We just send the message and allow the caller to specify what they do with it
		requestChan.ch <- msg
		if c.ph.respID != 0 {
			log.Printf("we have a responder to respond to -- %v", c.ph.respID)
			header := constructNodeHeader(1, OK, 0, c.ph.respID, uint16(len(respOK)), NODE_HEADER_SIZE_V1, 0, 0)
			packet := &nodePacket{
				header,
				respOK,
			}

			pay, err := packet.serialize()
			if err != nil {
				log.Printf("error serialising packet - %v", err)
			}

			c.mu.Lock()
			c.qProto(pay, true)
			c.mu.Unlock()
		}

	} else {
		log.Printf("no response channel found")
		// Else handle as normal command
	}

}

func (c *gbClient) processHandShake(message []byte) {

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		log.Printf("deserialise Delta failed: %v", err)
		// Send err response
	}

	// TODO Finish this
	// Send HandShake Response here
	// --
	info, err := c.srv.prepareSelfInfoSend(HANDSHAKE_RESP, int(c.ph.reqID), 0)
	if err != nil {
		log.Printf("prepareSelfInfoSend failed: %v", err)
	}

	c.mu.Lock()
	c.qProto(info, true)
	c.mu.Unlock()

	for key, value := range tmpC.delta {
		log.Printf("%s ---> %s - %+v", c.srv.ServerName, key, value)
		err := c.srv.addParticipantFromTmp(key, value)
		if err != nil {
			log.Printf("AddParticipantFromTmp failed: %v", err)
			//send err response
		}

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

	c.rh.rm.Lock()
	responseChan, exists := c.rh.resp[int(c.ph.reqID)]
	c.rh.rm.Unlock()

	msg := make([]byte, len(message))
	copy(msg, message)

	if exists {

		// We just send the message and allow the caller to specify what they do with it
		responseChan.ch <- msg

	} else {
		log.Printf("%s no response channel found", c.srv.ServerName)
		// Else handle as normal command
	}

	//resp := []byte("OK BOIII +\r\n")
	//
	//header := constructNodeHeader(1, OK, c.ph.id, uint16(len(resp)), NODE_HEADER_SIZE_V1, 0, 0)
	//packet := &nodePacket{
	//	header,
	//	resp,
	//}
	//
	//pay, err := packet.serialize()
	//if err != nil {
	//	log.Printf("error serialising packet - %v", err)
	//}
	//
	//c.mu.Lock()
	//c.qProto(pay, true)
	//c.mu.Unlock()

}

func (c *gbClient) processOK(message []byte) {

	log.Printf("resp id for ok == %v", int(c.ph.reqID))

	c.rh.rm.Lock()
	responseChan, exists := c.rh.resp[int(c.ph.reqID)]
	c.rh.rm.Unlock()

	msg := make([]byte, len(message))
	copy(msg, message)

	if exists {

		// We just send the message and allow the caller to specify what they do with it
		responseChan.ch <- msg

	} else {
		log.Printf("%s - no response channel found for OK + ", c.srv.ServerName)
		// Else handle as normal command
	}

}

func (c *gbClient) processGossAck(message []byte) {

}

func (c *gbClient) processGossSynAck(message []byte) {

}

func (c *gbClient) processGossSyn(message []byte) {

	//TODO We need to grab the server lock here and take a look at who we are gossiping with in order to see
	// if we need to defer gossip round or continue

	delta, err := deSerialiseDigest(message)
	if err != nil {
		log.Printf("error serialising digest - %v", err)
	}

	senderName := delta[0].senderName

	// Does the sending node need to defer?
	// If it does - then we must construct an error response, so it can exit out of it's round
	deferGossip, err := c.srv.deferGossipRound(senderName)
	if err != nil {
		log.Printf("error deferring gossip - %v", err)
		return
	}

	if deferGossip {

		log.Printf("%s making %s defer it's gossip", c.srv.ServerName, senderName)

		// TODO Package common error responses into a function (same with ok + responses)

		errHeader := constructNodeHeader(1, ERR_RESP, c.ph.reqID, 0, uint16(len(gossipError)), NODE_HEADER_SIZE_V1, 0, 0)
		errPacket := &nodePacket{
			errHeader,
			gossipError,
		}

		errPay, err := errPacket.serialize()
		if err != nil {
			log.Printf("error serialising packet - %v", err)
		}

		c.mu.Lock()
		c.qProto(errPay, true)
		c.mu.Unlock()

		return

	}

	header := constructNodeHeader(1, OK, c.ph.reqID, 0, uint16(len(respOK)), NODE_HEADER_SIZE_V1, 0, 0)
	packet := &nodePacket{
		header,
		respOK,
	}

	pay, err := packet.serialize()
	if err != nil {
		log.Printf("error serialising packet - %v", err)
	}

	c.mu.Lock()
	c.qProto(pay, true)
	c.mu.Unlock()

	return

}

func (c *gbClient) processInfoMessage(message []byte) {

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		log.Printf("deserialise Delta failed: %v", err)
		// Send err response
	}

	// TODO - node should check if message is of correct info - add to it's own cluster map and then respond
	// Allow for an error response or retry if this is not correct
	// TODO - then use method to add to cluster - must do check to see if it is in cluster already, if so we must call update instead

	//=========================================

	// TODO When we onboard new joiner we should wait for response before adding to cluster map

	err = c.onboardNewJoiner()
	if err != nil {
		log.Printf("onboardNewJoiner failed: %v", err)
	}

	// We have to do this last because we will end up sending back the nodes own info

	for key, value := range tmpC.delta {

		err := c.srv.addParticipantFromTmp(key, value)
		if err != nil {
			log.Printf("AddParticipantFromTmp failed: %v", err)
			//send err response
		}

	}

	// Move the tmpClient to connected as it has provided its info which we have now stored
	//err = c.srv.moveToConnected(c.cid, tmpC.sender)
	//if err != nil {
	//	log.Printf("MoveToConnected failed in process info message: %v", err)
	//}

	// TODO Monitor the server lock here and be mindful
	//c.srv.incrementNodeConnCount()

	return

}
