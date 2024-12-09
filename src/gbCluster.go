package src

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
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

//-------------------
//Digest for initial gossip - per connection/node - will be passed as []*clusterDigest

type clusterDigest struct {
	name       string
	maxVersion int64
}

//-------------------
// Main cluster map for gossiping

type Delta struct {
	valueType int
	version   int64
	value     []byte // Value should go last for easier de-serialisation
}

type Participant struct {
	name       string
	keyValues  map[int]*Delta
	maxVersion int64
	paValue    float64 // Not to be gossiped
	pm         sync.RWMutex
}

type ClusterMap struct {
	seedServer   *Seed
	participants map[string]*Participant
	phiAccMap    map[string]*phiAccrual
	pCount       int
	cachedDigest map[string]*clusterDigest
}

//-------------------
//Heartbeat Monitoring

type phiAccrual struct {
	threshold   int
	windowSize  int
	lastBeat    int64
	currentBeat int64
	window      map[int]int64
	pa          sync.Mutex
}

// -- Maybe a PhiAcc map with node-name as key and phiAccrual as value?

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
	clusterDigest //TODO May not need
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
		log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.LocalAddr())
		// TODO if the client initiated the connection and is a new NODE then it must send info on first message

	} else {
		client.directionType = RECEIVED
		log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.RemoteAddr())

		//var testData = []byte{1, 1, 1, 0, 16, 0, 9, 13, 10, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 13, 10}
		//
		////time.Sleep(1 * time.Second)
		//client.qProto(testData, false)
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

	////Create info message
	data := []byte("This is a test\r\n")

	seq, err := s.acquireReqID()
	if err != nil {
		return err
	}

	header1 := constructNodeHeader(1, 1, seq, uint16(len(data)), NODE_HEADER_SIZE_V1)
	packet := &nodePacket{
		header1,
		data,
	}
	pay1, err := packet.serialize()
	if err != nil {
		fmt.Printf("Failed to serialize packet: %v\n", err)
	}
	//
	log.Printf("%v", len(pay1))

	addr := net.JoinHostPort(s.gbConfig.SeedServers[0].SeedIP, s.gbConfig.SeedServers[0].SeedPort)

	fmt.Printf("%s Attempting to connect to seed server: %s\n", s.ServerName, addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}

	log.Println("connection to seed successful - ", conn.RemoteAddr())

	client := s.createNodeClient(conn, "whaaaat", true, NODE)

	client.qProto(pay1, false)

	// Flushing here as we may be earlier than signal setup
	client.mu.Lock()
	client.flushWriteOutbound()
	client.mu.Unlock()

	//conn.Write(pay1)

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

func initSelfParticipant(name, addr string) *Participant {

	t := time.Now().Unix()

	p := &Participant{
		name:       name,
		keyValues:  make(map[int]*Delta),
		maxVersion: t,
	}

	p.keyValues[ADDR_V] = &Delta{
		valueType: STRING_DV,
		version:   t,
		value:     []byte(addr),
	}
	// Set the numNodeConnections delta
	numNodeConnBytes := make([]byte, 1)
	numNodeConnBytes[0] = 0
	p.keyValues[NUM_NODE_CONN_V] = &Delta{
		valueType: INT_DV,
		version:   t,
		value:     numNodeConnBytes,
	}

	heart := make([]byte, 8)
	binary.BigEndian.PutUint64(heart, uint64(t))
	p.keyValues[HEARTBEAT_V] = &Delta{
		valueType: HEARTBEAT_V,
		version:   t,
		value:     heart,
	}

	// TODO need to figure how to update maxVersion - won't be done here as this is the lowest version

	return p

}

func initClusterMap(name string, seed *net.TCPAddr, participant *Participant) *ClusterMap {

	cm := &ClusterMap{
		&Seed{seedAddr: seed},
		make(map[string]*Participant),
		make(map[string]*phiAccrual),
		0,
		make(map[string]*clusterDigest),
	}

	// We don't add the phiAccrual here as we don't track our own internal failure detection

	cm.participants[name] = participant
	cm.pCount++

	return cm

}

// TODO Consider a digest pool to use to ease pressure on the Garbage Collector

// Thread safe and to be used when cached digest is nil or invalidated
func (s *GBServer) generateDigest() (map[string]*clusterDigest, error) {

	s.clusterLock.RLock()
	defer s.clusterLock.RUnlock()

	if s.clusterMap.participants == nil {
		return nil, fmt.Errorf("cluster map is empty")
	}

	if len(s.clusterMap.cachedDigest) > 0 {
		return s.clusterMap.cachedDigest, nil
	}

	td := make(map[string]*clusterDigest, s.clusterMap.pCount)

	cm := s.clusterMap.participants

	for _, value := range cm {
		// Lock the participant to safely read the data
		value.pm.RLock()
		// Initialize the map entry for each participant
		td[value.name] = &clusterDigest{
			name:       value.name,
			maxVersion: value.maxVersion,
		}
		value.pm.RUnlock() // Release the participant lock
	}

	s.clusterMap.cachedDigest = td

	return td, nil
}
