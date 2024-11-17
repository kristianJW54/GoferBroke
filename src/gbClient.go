package src

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"sync"
	"time"
)

const (
	CLIENT = iota
	NODE
)

const (
	INITIATED = "Initiated"
	RECEIVED  = "Received"
)

//===================================================================================
// Client
//===================================================================================

type gbClient struct {
	Name    string
	created time.Time

	srv *GBServer

	// Conn is both node and client
	gbc net.Conn
	// Inbound is a read cache for wire reads
	inbound readCache

	// cType determines if the conn is a node or client
	cType int
	// directionType determines if the conn was initiated (dialed by this server) or received (accepted in the accept loop)
	directionType string

	//Routing info
	nodeUrl *url.URL

	//Syncing
	cLock sync.RWMutex
}

//===================================================================================
// Read Cache
//===================================================================================

type readCache struct {
	buffer      []byte
	offset      int
	expandCount int
	buffSize    int
}

//===================================================================================
// Outbound Write - For Fan-In - Per connection [Node]
//===================================================================================

const (
	SMALL_OUT_BUFFER    = 512
	STANDARD_OUT_BUFFER = 1024
	MEDIUM_OUT_BUFFER   = 2048
	LARGE_OUT_BUFFER    = 4096
)

type outboundNodeQueue struct {
	bytesInQ    uint64
	writeBuffer net.Buffers
	flushSignal *sync.Cond
	outLock     *sync.RWMutex
}

//===================================================================================
// Client creation
//===================================================================================

func (s *GBServer) createClient(conn net.Conn, name string, initiated bool, clientType int) *gbClient {

	defer conn.Close()

	client := &gbClient{
		Name:  name,
		srv:   s,
		gbc:   conn,
		cType: clientType,
	}

	// Only log if the connection was initiated by this server (to avoid duplicate logs)
	if initiated {
		client.directionType = INITIATED
		log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, name, clientType, conn.LocalAddr())
	} else {
		client.directionType = RECEIVED
		log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, name, clientType, conn.RemoteAddr())
	}

	// Read Loop for connection - reading and parsing off the wire and queueing to write if needed
	//go func() {
	//	client.readLoop()
	//}()
	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.ServerName, fmt.Sprintf("read loop for %s", name), func() {
		client.readLoop()
	})

	//Write loop -

	return client

}

//===================================================================================
// Client Connection + Wire Handling
//===================================================================================

//---------------------------
//Read Loop

func (c *gbClient) readLoop() {

	defer c.srv.grWg.Done()

	// Read and parse inbound messages

	//Check locks and if anything is closed or shutting down

	buf := make([]byte, INITIAL_BUFF_SIZE)

	var reader io.Reader
	reader = c.gbc

	//TODO Look at implementing a specific read function to handle our TCP Packets and have
	// our own protocol specific rules around read

	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			log.Println("read error", err)
			c.gbc.Close()
			return
		}
		if n == 0 {
			return
		}

		//TODO Implement a handler router for server-server connections and client-server connections
		// Similar to Nats where the read and write loop are run inside the handle (or in NATS case the connFunc)

		// Create a GossipPayload to unmarshal the received data into
		dataPayload := &TCPPacket{&PacketHeader{}, buf} //TODO This needs to a function to create a buffered payload

		err = dataPayload.UnmarshallBinaryV1(buf[:n]) // Read the exact number of bytes
		if err != nil {
			log.Println("unmarshall error", err)
			return
		}

		// Log the decoded data (as a string)
		log.Printf(
			"%v %v %v %v %s",
			dataPayload.Header.ProtoVersion,
			dataPayload.Header.Command,
			dataPayload.Header.MsgLength,
			dataPayload.Header.PayloadLength,
			string(dataPayload.Data),
		)
	}

}

//---------------------------
//Write Loop

func (c *gbClient) writeLoop() {

}
