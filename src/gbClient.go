package src

import (
	"bufio"
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

const (
	SMALL_OUT_BUFFER    = 512
	STANDARD_OUT_BUFFER = 1024
	MEDIUM_OUT_BUFFER   = 2048
	LARGE_OUT_BUFFER    = 4096
)

//===================================================================================
// Client | Node
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
// Outbound Node Write - For Per connection [Node] During Gossip Exchange
//===================================================================================

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
		defer conn.Close()
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

	c.cLock.Lock()
	if c.gbc == nil {
		c.cLock.Unlock()
		return
	}

	// Read and parse inbound messages
	// Check locks and if anything is closed or shutting down

	c.inbound.buffSize = MIN_BUFF_SIZE
	c.inbound.buffer = make([]byte, c.inbound.buffSize)
	buff := c.inbound.buffer

	reader := bufio.NewReader(c.gbc)

	// TODO: Look at implementing a specific read function to handle our TCP Packets and have
	// our own protocol-specific rules around read

	//------------------
	//Beginning the read loop - read into buffer and adjust size if necessary - parse and process
	for {

		c.cLock.Lock()

		// Adjust buffer if we're close to filling it up
		if c.inbound.offset >= cap(buff) && cap(buff) < MAX_BUFF_SIZE {
			c.inbound.buffSize = cap(buff) * 2
			if c.inbound.buffSize > MAX_BUFF_SIZE {
				c.inbound.buffSize = MAX_BUFF_SIZE
			}
			newBuffer := make([]byte, c.inbound.buffSize)
			copy(newBuffer, buff[:c.inbound.offset]) // Copy data into the new buffer
			buff = newBuffer                         // Assign new buffer to the inbound buffer
			log.Printf("increased buffer size to --> %d", c.inbound.buffSize)
		} else if c.inbound.offset < cap(buff)/2 && cap(buff) > MIN_BUFF_SIZE && c.inbound.expandCount > 2 {
			c.inbound.buffSize = int(cap(buff) / 2)
			newBuffer := make([]byte, c.inbound.buffSize)
			copy(newBuffer, buff[:c.inbound.offset]) // Copy data into the new buffer
			buff = newBuffer                         // Assign new buffer to the inbound buffer
			log.Printf("decreased buffer size to --> %d", c.inbound.buffSize)
		}

		c.cLock.Unlock()

		// Read data into buffer starting at the current offset
		n, err := reader.Read(buff[c.inbound.offset:])
		if err != nil {
			if err == io.EOF {
				log.Println("connection closed")
				return
			}
			log.Printf("read error: %s", err)
			return
		}

		// Check if we need to expand or shrink the buffer - if the buffer is half empty more than twice, we shrink
		if n > cap(buff) {
			c.inbound.expandCount = 0
			log.Printf("reseting expand count to: %d", c.inbound.expandCount)
		} else if n < cap(buff)/2 {
			c.inbound.expandCount++
			log.Printf("increasing expand count to: %d", c.inbound.expandCount)
		}

		log.Printf("raw data string: %s", string(buff[c.inbound.offset:c.inbound.offset+n]))

		log.Printf("data: %v", buff)

		// Update offset
		c.inbound.offset += n

		//-----------------------------
		// Parsing the packet - we send the buffer to be parsed, if we hit CLRF (for either header or data)
		// then we have complete packet and can call processing handlers
		// TODO: Add parsing here to check for complete packet and process

		log.Printf("bytes read --> %d", n)
		log.Printf("current buffer usage --> %d / %d", c.inbound.offset, len(buff))

		// TODO Think about flushing and writing and any clean up after the read

		//// Create a GossipPayload to unmarshal the received data into
		//dataPayload := &TCPPacket{&PacketHeader{}, buf} //TODO This needs to a function to create a buffered payload
		//
		//err = dataPayload.UnmarshallBinaryV1(buf[:n]) // Read the exact number of bytes
		//if err != nil {
		//	log.Println("unmarshall error", err)
		//	return
		//}
		//
		//// Log the decoded data (as a string)
		//log.Printf(
		//	"%v %v %v %v %s",
		//	dataPayload.Header.ProtoVersion,
		//	dataPayload.Header.Command,
		//	dataPayload.Header.MsgLength,
		//	dataPayload.Header.PayloadLength,
		//	string(dataPayload.Data),
		//)
	}

}

//---------------------------
//Write Loop

func (c *gbClient) writeLoop() {

	// Will have node writes and client writes
	// Node writes will have a single output queue outputting during gossip exchange
	// Client writes will be fan-in > fan-out pattern to interested clients to write to

}
