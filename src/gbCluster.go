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

type Seed struct {
	seedAddr *net.TCPAddr
}

type Delta struct {
	key     string
	value   string
	version int64
}

type Participant struct {
	name       string
	deltas     map[string][]Delta
	maxVersion int64
}

type ClusterMap struct {
	seedServer   Seed
	participants map[string]Participant
}

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

	// Only log if the connection was initiated by this server (to avoid duplicate logs)
	if initiated {
		client.directionType = INITIATED
		log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.LocalAddr())
	} else {
		client.directionType = RECEIVED
		log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.RemoteAddr())
	}

	log.Println(s.ServerName + ": storing " + client.Name)
	s.tmpClientStore["1"] = client

	//May want to update some node connection  metrics which will probably need a write lock from here
	// Node count + connection map

	// Initialise read caches and any buffers and store info
	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.ServerName, fmt.Sprintf("read loop for %s", name), func() {
		client.readLoop()
	})

	// Also a write loop

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

	addr := net.JoinHostPort(s.gbConfig.SeedServers[0].SeedIP, s.gbConfig.SeedServers[0].SeedPort)

	fmt.Printf("%s Attempting to connect to seed server: %s\n", s.ServerName, addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}

	_, err = conn.Write(pay1) // Sending the packet //TODO can this be added to an outbound queue?
	if err != nil {
		return fmt.Errorf("error writing to connection: %s", err)
	}

	//Once it has successfully dialled we want to create a node client and store the connection
	// + wait for info exchange + dial back

	s.createNodeClient(conn, "whaaaat", true, NODE)

	select {
	case <-ctx.Done():
		log.Println("connect to seed cancelled because of context")
		s.releaseReqID(seq)
	}

	return nil

}
