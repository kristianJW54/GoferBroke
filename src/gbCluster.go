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
	HEARTBEAT_V = iota
	ADDR_V
	CPU_USAGE_V
	MEMORY_USAGE_V
	NUM_NODE_CONN_V
	NUM_CLIENT_CONN_V
	INTEREST_V
	ROUTES_V
)

// Internal Delta Keys [NOT TO BE USED EXTERNALLY]
const (
	_ADDRESS_      = "ADDR"
	_CPU_USAGE_    = "CPU_USAGE"
	_MEMORY_USAGE  = "MEMORY_USAGE"
	_NODE_CONNS_   = "NODE_CONNS"
	_CLIENT_CONNS_ = "CLIENT_CONNS"
	_HEARTBEAT_    = "HEARTBEAT"
)

type Seed struct {
	seedAddr *net.TCPAddr
}

//-------------------
// Main cluster map for gossiping

type Delta struct {
	valueType int
	version   int64
	value     []byte // Value should go last for easier de-serialisation
}

type Participant struct {
	name       string // Possibly can remove
	keyValues  map[string]*Delta
	valueIndex []string
	maxVersion int64
	paValue    float64 // Not to be gossiped
	pm         sync.RWMutex
}

type ClusterMap struct {
	seedServer   *Seed
	participants map[string]*Participant
	partIndex    []string
	phiAccMap    map[string]*phiAccrual
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
		//log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.LocalAddr())
		// TODO if the client initiated the connection and is a new NODE then it must send info on first message

	} else {
		client.directionType = RECEIVED
		//log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.RemoteAddr())

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

//=======================================================
// Node Info + Initial Connect Packet Creation
//=======================================================

func initSelfParticipant(name, addr string) *Participant {

	t := time.Now().Unix()

	p := &Participant{
		name:       name,
		keyValues:  make(map[string]*Delta),
		valueIndex: make([]string, 3),
		maxVersion: t,
	}

	p.keyValues[_ADDRESS_] = &Delta{
		valueType: STRING_DV,
		version:   t,
		value:     []byte(addr),
	}
	p.valueIndex[0] = _ADDRESS_

	// Set the numNodeConnections delta
	numNodeConnBytes := make([]byte, 1)
	numNodeConnBytes[0] = 0
	p.keyValues[_NODE_CONNS_] = &Delta{
		valueType: INT_DV,
		version:   t,
		value:     numNodeConnBytes,
	}
	p.valueIndex[1] = _NODE_CONNS_

	heart := make([]byte, 8)
	binary.BigEndian.PutUint64(heart, uint64(t))
	p.keyValues[_HEARTBEAT_] = &Delta{
		valueType: HEARTBEAT_V,
		version:   t,
		value:     heart,
	}
	p.valueIndex[2] = _HEARTBEAT_

	// TODO need to figure how to update maxVersion - won't be done here as this is the lowest version

	return p

}

//=======================================================
// Cluster Map Handling
//=======================================================

func initClusterMap(name string, seed *net.TCPAddr, participant *Participant) *ClusterMap {

	cm := &ClusterMap{
		&Seed{seedAddr: seed},
		make(map[string]*Participant),
		make([]string, 0),
		make(map[string]*phiAccrual),
	}

	// We don't add the phiAccrual here as we don't track our own internal failure detection

	cm.participants[name] = participant
	cm.partIndex = append(cm.partIndex, name)

	return cm

}

func (s *GBServer) prepareInfoSend() ([]byte, error) {

	s.clusterLock.Lock()
	defer s.clusterLock.Unlock()

	// Check if the server name exists in participants
	participant, ok := s.clusterMap.participants[s.ServerName]
	if !ok {
		return nil, fmt.Errorf("no participant found for server %s", s.ServerName)
	}

	log.Println("STARTED PREPARING")

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
	if err != nil {
		return nil, err
	}

	return pay1, nil

}

//--------
//Update cluster

//--

//Add/Remove Participant

//--

// TODO Consider a digest pool to use to ease pressure on the Garbage Collector

// Thread safe and to be used when cached digest is nil or invalidated
func (s *GBServer) generateDigest() ([]*clusterDigest, error) {

	s.clusterLock.RLock()
	defer s.clusterLock.RUnlock()

	if s.clusterMap.participants == nil {
		return nil, fmt.Errorf("cluster map is empty")
	}

	td := make([]*clusterDigest, len(s.clusterMap.participants))

	cm := s.clusterMap.participants

	idx := 0
	for _, value := range cm {
		// Lock the participant to safely read the data
		value.pm.RLock()
		// Initialize the map entry for each participant
		td[idx] = &clusterDigest{
			name:       value.name,
			maxVersion: value.maxVersion,
		}
		idx++
		value.pm.RUnlock() // Release the participant lock
	}

	return td, nil
}

//--------
//Compare Digest
