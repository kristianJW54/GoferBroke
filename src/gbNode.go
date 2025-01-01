package src

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
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

// createNodeClient method belongs to the server which receives the connection from the connecting server
func (s *GBServer) createNodeClient(conn net.Conn, name string, initiated bool, clientType int) *gbClient {

	now := time.Now()
	clientName := fmt.Sprintf("%s_%d", name, now.Unix())

	client := &gbClient{
		Name:    clientName,
		created: now,
		srv:     s,
		gbc:     conn,
		cType:   clientType,
	}

	s.serverLock.Lock()
	s.numNodeConnections++
	s.serverLock.Unlock()

	client.mu.Lock()
	client.initClient()
	client.mu.Unlock()

	s.serverLock.Lock()
	s.tmpClientStore[client.cid] = client
	s.serverLock.Unlock()

	//May want to update some node connection  metrics which will probably need a write lock from here
	// Node count + connection map

	// Initialise read caches and any buffers and store info
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
		//log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.LocalAddr())
		// TODO if the client initiated the connection and is a new NODE then it must send info on first message

	} else {
		client.directionType = RECEIVED
		//log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.RemoteAddr())

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

	ctx, cancel := context.WithTimeout(s.serverContext, 10*time.Second)
	defer cancel()

	addr := net.JoinHostPort(s.gbConfig.SeedServers[0].SeedIP, s.gbConfig.SeedServers[0].SeedPort)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}

	pay1, err := s.prepareInfoSend()
	if err != nil {
		return err
	}

	client := s.createNodeClient(conn, "whaaaat", true, NODE)

	//TODO Need to wait here until we receive the OK from seed servers that we have been onboarded and can proceed
	// the ok will be from a complete cluster info send with a OK + EOS message signalling we have got all the info to begin
	rsp, err := client.qProtoWithResponse(pay1, false, true)
	if err != nil {
		return err
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
	s.serverLock.Lock()
	err = s.moveToConnected(client.cid)
	if err != nil {
		return err
	}
	s.serverLock.Unlock()

	select {
	case <-ctx.Done():
		log.Println("connect to seed cancelled because of context")
		return ctx.Err()
	default:
	}

	return nil

}

// Thread safe
func (s *GBServer) prepareInfoSend() ([]byte, error) {

	s.clusterMapLock.Lock()
	defer s.clusterMapLock.Unlock()

	// Check if the server name exists in participants
	participant, ok := s.clusterMap.participants[s.ServerName]
	if !ok {
		return nil, fmt.Errorf("no participant found for server %s", participant.name)
	}

	// TODO Can we serialise straight from self info and avoid creating temp structures? let the receiver do it

	// Need to serialise the tmpCluster
	cereal, err := s.serialiseClusterDelta(nil)
	if err != nil {
		return nil, err
	}

	// Acquire sequence ID
	seq, err := s.acquireReqID()
	if err != nil {
		return nil, err
	}

	// Construct header
	header := constructNodeHeader(1, INFO, seq, uint16(len(cereal)), NODE_HEADER_SIZE_V1, 0, 0)
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

	if len(s.clusterMap.partIndex) > 100 {
		log.Printf("lots of participants - may need more efficient snapshot transfer")
		// In this case we would send an INFO_ACK to tell the joining node that more info will come
		// The joining node can then reach out to other seed servers or send another request to this seed server
		// With the digest of what it has been given so far to complete it's joining
		// A mini bootstrapped gossip between joiner and seeds

		// TODO If cluster too large - will need to stream data and use metadata to track progress
	}

	s.clusterMapLock.Lock()
	msg, err := s.serialiseClusterDelta(nil)
	if err != nil {
		return err
	}
	s.clusterMapLock.Unlock()

	hdr := constructNodeHeader(1, INFO_ALL, c.ph.id, uint16(len(msg)), NODE_HEADER_SIZE_V1, 0, 0)

	packet := &nodePacket{
		hdr,
		msg,
	}

	pay1, err := packet.serialize()
	if err != nil {
		return err
	}

	c.qProto(pay1, true)

	return nil

}

//===================================================================================
// Parser Header Processing
//===================================================================================

func (c *gbClient) processArg(arg []byte) error {
	// Assuming the first 3 bytes represent the command and the next bytes represent msgLength

	// TODO may need a arg dispatcher or different arg processors

	if len(arg) >= 4 {
		c.ph.version = arg[0]
		c.ph.id = arg[2]
		c.ph.command = arg[1]
		// Extract the last 4 bytes
		msgLengthBytes := arg[3:5]
		// Convert those 4 bytes to uint32 (BigEndian)
		c.ph.msgLength = int(binary.BigEndian.Uint16(msgLengthBytes))

		// Log the result to verify
		//log.Printf("Extracted msgLength: %d\n", c.ph.msgLength)
	} else {
		return fmt.Errorf("argument does not have enough bytes to extract msgLength")
	}

	c.argBuf = arg

	return nil
}

//===================================================================================
// Parser Message Processing - Dispatched from processMessage()
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

}

func (c *gbClient) processInfoAll(message []byte) {

	c.rm.Lock()
	responseChan, exists := c.resp[int(c.argBuf[2])]
	c.rm.Unlock()

	if exists {

		// We just send the message and allow the caller to specify what they do with it
		responseChan.ch <- message

		c.rm.Lock()
		delete(c.resp, int(c.argBuf[2]))
		c.rm.Unlock()

	} else {
		log.Printf("no response channel found")
		// Else handle as normal command
	}

}

func (c *gbClient) processOK(message []byte) {

	c.rm.Lock()
	responseChan, exists := c.resp[int(c.argBuf[2])]
	c.rm.Unlock()

	if exists {

		err := errors.New("this is an error response TEST")
		responseChan.err <- err

		c.rm.Lock()
		delete(c.resp, int(c.argBuf[2]))
		c.rm.Unlock()

	} else {
		log.Printf("no response channel found")
	}

}

func (c *gbClient) processGossAck(message []byte) {

}

func (c *gbClient) processGossSynAck(message []byte) {

}

func (c *gbClient) processGossSyn(message []byte) {

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

	err = c.onboardNewJoiner()
	if err != nil {
		log.Printf("onboardNewJoiner failed: %v", err)
	}

	// If this is one participant consider accessing another way than loop

	// We have to do this last because we will end up sending back the nodes own info
	for key, value := range tmpC.delta {

		err := c.srv.addParticipantFromTmp(key, value)
		if err != nil {
			log.Printf("AddParticipantFromTmp failed: %v", err)
			//send err response
		}

		// Move the tmpClient to connected as it has provided its info which we have now stored
		err = c.srv.moveToConnected(c.cid)
		if err != nil {
			log.Printf("MoveToConnected failed: %v", err)
		}

	}

	return

}
