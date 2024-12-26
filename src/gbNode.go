package src

import (
	"context"
	"encoding/binary"
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

	//Server Lock?
	s.numNodeConnections++

	client.mu.Lock()
	defer client.mu.Unlock()

	client.initClient()

	//log.Println(s.ServerName + ": storing " + client.Name)
	s.tmpClientStore["1"] = client

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

// TODO This is where we will wait for a response in a non blocking way and use the req ID - upon response, we will release the ID back to the pool
// will need a ID map for active request awaiting responses and handlers for when is done or timeout reached then auto release
func (s *GBServer) connectToSeed() error {

	//With this function - we reach out to seed - so in our connection handling we would need to check protocol version
	//To understand how this connection is communicating ...

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

	client.qProtoWithResponse(pay1, false, true)

	// TODO should move to createNodeClient?
	select {
	case <-ctx.Done():
		log.Println("connect to seed cancelled because of context")
		return ctx.Err()
	default:
	}

	return nil

}

func (s *GBServer) prepareInfoSend() ([]byte, error) {

	s.clusterMapLock.Lock()
	defer s.clusterMapLock.Unlock()

	// Check if the server name exists in participants
	participant, ok := s.clusterMap.participants[s.ServerName]
	if !ok {
		return nil, fmt.Errorf("no participant found for server %s", s.ServerName)
	}

	log.Println("STARTED PREPARING")

	// TODO Can we serialise straight from self info and avoid creating temp structures? let the receiver do it

	// Setup tmpCluster
	tmpC := &clusterDelta{make(map[string]*tmpParticipant, 1)}

	// Lock the participant for reading
	participant.pm.RLock()
	defer participant.pm.RUnlock() // Ensure the read lock is released even on errors

	// Capture the indexes
	pi := s.clusterMap.partIndex

	tmpP := &tmpParticipant{keyValues: make(map[string]*Delta, len(participant.keyValues)), vi: participant.valueIndex}

	tmpC.delta[s.ServerName] = tmpP

	for _, v := range participant.valueIndex {
		// Copy keyValues into tmpParticipant
		tmpP.keyValues[v] = participant.keyValues[v]
	}

	// Need to serialise the tmpCluster
	cereal, err := serialiseClusterDelta(tmpC, pi)
	if err != nil {
		return nil, err
	}

	// Acquire sequence ID
	seq, err := s.acquireReqID()
	log.Printf("seq ID = %d", seq)
	if err != nil {
		return nil, err
	}

	// Construct header
	header := constructNodeHeader(1, INFO, seq, uint16(len(cereal)), NODE_HEADER_SIZE_V1)
	// Create packet
	packet := &nodePacket{
		header,
		cereal,
	}
	pay1, err := packet.serialize()
	log.Printf("pay1 %v", pay1)
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

//===================================================================================
// Parser Header Processing
//===================================================================================

func (c *gbClient) processINFO(arg []byte) error {
	// Assuming the first 3 bytes represent the command and the next bytes represent msgLength

	if len(arg) >= 4 {
		c.ph.version = arg[0]
		c.ph.id = arg[2]
		c.ph.command = arg[1]
		// Extract the last 4 bytes
		msgLengthBytes := arg[3:5]
		// Convert those 4 bytes to uint32 (BigEndian)
		c.ph.msgLength = int(binary.BigEndian.Uint16(msgLengthBytes))

		// Log the result to verify
		log.Printf("Extracted msgLength: %d\n", c.ph.msgLength)
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
		c.processInitialMessage(message)
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

func (c *gbClient) processOK(message []byte) {

	//log.Printf("returned message = %s", string(message))
	c.rm.Lock()
	responseChan, exists := c.resp[int(c.argBuf[2])]
	c.rm.Unlock()

	if exists {

		responseChan <- message

		c.rm.Lock()
		delete(c.resp, int(c.argBuf[0]))
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

func (c *gbClient) processInitialMessage(message []byte) {

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		log.Printf("deserialiseDelta failed: %v", err)
	}
	for key, value := range tmpC.delta {
		log.Printf("key = %s", key)
		for k, v := range value.keyValues {
			log.Printf("value[%v]: %v", k, v)
		}
	}

	// TODO - node should check if message is of correct info - add to it's own cluster map and then respond

	cereal := []byte("OK +\r\n")

	// Construct header
	header := constructNodeHeader(1, OK, c.ph.id, uint16(len(cereal)), NODE_HEADER_SIZE_V1)
	// Create packet
	packet := &nodePacket{
		header,
		cereal,
	}
	pay1, _ := packet.serialize()

	c.qProto(pay1, true)

	return

}
