package src

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

//===================================================================================
// Cluster Map
//===================================================================================

const (
	STRING_DV = iota
	UINT8_DV
	UINT16_DV
	UINT32_DV
	UINT64_DV
	INT_DV
	BYTE_DV
	FLOAT_DV
	TIME_DV
)

const (
	HEARTBEAT_V = iota
	ADDR_V
	CPU_USAGE_V
	MEMORY_USAGE_V
	NUM_NODE_CONN_V
	NUM_CLIENT_CONN_V
	INTEREST_V
	ROUTES_V
)

type Seed struct {
	seedAddr *net.TCPAddr
}

type Delta struct {
	valueType int
	version   int64
	value     []byte // Value should go last for easier de-serialisation
}

type Participant struct {
	name       string
	keyValues  map[int]*Delta
	maxVersion int64
}

type ClusterMap struct {
	seedServer   Seed
	participants map[string]*Participant
}

// TODO Think about functions or methods which will take new participant data and serialise/de-serialise it for adding to map
// TODO Think about where these functions need to live and how to handle

// handler for first gossip round
// command will be syn
// inside - will need to create a digest and queue it, then wait for response
// once response given - it will be syn-ack, which we will need to call the ack handler and process etc

// if syn received - need to call syn-ack handler which will generate a digest and a delta to queue, response will be an ack

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

	//Serialisation + Gossip
	tmpDigest
	gossipingWith string
	// Gossip state - Handlers will check against this and make sure no duplicate or conflicting work is being done
	gossipState int
}

//===================================================================================
// Node Connection
//===================================================================================

//-------------------------------
// Creating a node server
//-------------------------------

// createNode is the entry point to reading and writing
// createNode will have a read write loop
// createNode lives inside the node accept loop

// createNodeClient method belongs to the server which receives the connection from the connecting server
func (s *GBServer) createNodeClient(conn net.Conn, name string, initiated bool, clientType int) *gbClient {

	// TODO Think about locks

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

	log.Printf("lock acquired - create node client")

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
		log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.LocalAddr())
		// TODO if the client initiated the connection and is a new NODE then it must send info on first message
		//var testData = []byte{1, 1, 1, 0, 16, 0, 9, 13, 10, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 13, 10}
		//
		//client.qProto(testData, true)

	} else {
		client.directionType = RECEIVED
		log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.RemoteAddr())

	}

	log.Printf("lock released - create node client")

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

	////Create info message
	//data := []byte("This is a test\r\n")
	//
	//seq, err := s.acquireReqID()
	//if err != nil {
	//	return err
	//}
	//
	//header1 := constructNodeHeader(1, 1, seq, uint16(len(data)), NODE_HEADER_SIZE_V1)
	//packet := &nodePacket{
	//	header1,
	//	data,
	//}
	//pay1, err := packet.serialize()
	//if err != nil {
	//	fmt.Printf("Failed to serialize packet: %v\n", err)
	//}
	//
	//log.Printf("%v", len(pay1))

	addr := net.JoinHostPort(s.gbConfig.SeedServers[0].SeedIP, s.gbConfig.SeedServers[0].SeedPort)

	fmt.Printf("%s Attempting to connect to seed server: %s\n", s.ServerName, addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}

	log.Println("connection to seed successful - ", conn.RemoteAddr())

	// TODO Add this to outbound queue instead and let flush outbound handle the write
	//_, err = conn.Write(pay1)
	//if err != nil {
	//	return fmt.Errorf("error writing to connection: %s", err)
	//}

	//Once it has successfully dialled we want to create a node client and store the connection
	// + wait for info exchange

	s.createNodeClient(conn, "whaaaat", true, NODE)
	//client.queueOutbound(pay1)
	//client.flushWriteOutbound()
	//client.qProto(pay1, true)

	// TODO should move to createNodeClient?
	select {
	case <-ctx.Done():
		log.Println("connect to seed cancelled because of context")
		//s.releaseReqID(seq)
	}

	return nil

}

//=======================================================
// Node Info + Initial Connect Packet Creation
//=======================================================

func (s *GBServer) initSelfParticipant() {

	t := time.Now().Unix()

	p := &Participant{
		name:       s.ServerName,
		keyValues:  make(map[int]*Delta),
		maxVersion: t,
	}

	p.keyValues[ADDR_V] = &Delta{
		valueType: STRING_DV,
		version:   t,
		value:     []byte(s.addr),
	}
	// Set the numNodeConnections delta
	numNodeConnBytes := make([]byte, 1)
	numNodeConnBytes[0] = s.numNodeConnections
	p.keyValues[NUM_NODE_CONN_V] = &Delta{
		valueType: INT_DV,
		version:   t,
		value:     numNodeConnBytes,
	}

	// TODO need to figure how to update maxVersion - won't be done here as this is the lowest version

	s.serverLock.Lock()
	s.selfInfo = p
	s.serverLock.Unlock()

}

// TODO Think about how to keep the internal state up to date for gossiping
