package src

import (
	"log"
	"net"
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
// Node Connection
//===================================================================================

//-------------------------------
// Creating a node server
//-------------------------------

// createNode is the entry point to reading and writing
// createNode will have a read write loop
//createNode lives inside the node accept loop

func (s *GBServer) createNodeClient(conn net.Conn, name string, clientType int) *gbClient {

	//May want to get server config or options here
	log.Printf("creating connection --> %s --> type: %d\n", name, clientType)

	client := &gbClient{
		Name:  name,
		srv:   s,
		gbc:   conn,
		cType: clientType,
	}

	// Initialise read caches and any buffers and store info
	go func() {
		defer conn.Close() // TODO Fine for now but need to properly manage within the read loop
		client.readLoop()  //Can take in a pre buffer for later tls ??
	}()

	// Also a write loop

	return client

}
