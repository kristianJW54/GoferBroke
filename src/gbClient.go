package src

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	CLIENT = iota
	NODE
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
	// Outbound is an outbound struct for queueing to write loop and flushing
	outbound outboundNodeQueue

	// cType determines if the conn is a node or client
	cType int
	// directionType determines if the conn was initiated (dialed by this server) or received (accepted in the accept loop)
	directionType string
	// Node client
	node

	//Parsing + State
	stateMachine

	//Flags --> will tell us what state the client is in (connected, awaiting_syn_ack, etc...)
	flags int

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

// TODO Should the outbound be a general all purpose connection outbound?
//===================================================================================
// Outbound Node Write - For Per connection [Node] During Gossip Exchange
//===================================================================================

type outboundNodeQueue struct {
	bytesInQ    uint64
	writeBuffer net.Buffers
	flushSignal *sync.Cond
	outLock     *sync.RWMutex
	sw          *bufio.Writer
}

const (
	NodeWritePoolSmall  = 512
	NodeWritePoolMedium = 2048
	NodeWritePoolLarge  = 4096
)

var nodePoolSmall = &sync.Pool{
	New: func() any {
		b := [NodeWritePoolSmall]byte{}
		return &b
	},
}

var nodePoolMedium = &sync.Pool{
	New: func() any {
		b := [NodeWritePoolMedium]byte{}
		return &b
	},
}

var nodePoolLarge = &sync.Pool{
	New: func() any {
		b := [NodeWritePoolLarge]byte{}
		return &b
	},
}

func nodePoolGet(size int) []byte {
	switch {
	case size <= NodeWritePoolSmall:
		return nodePoolSmall.Get().(*[NodeWritePoolSmall]byte)[:0]
	case size <= NodeWritePoolMedium:
		return nodePoolMedium.Get().(*[NodeWritePoolMedium]byte)[:0]
	default:
		return nodePoolLarge.Get().(*[NodeWritePoolLarge]byte)[:0]
	}
}

//===================================================================================
// Client creation
//===================================================================================

// TODO Think about the locks we may need in this method
func (s *GBServer) createClient(conn net.Conn, name string, initiated bool, clientType int) *gbClient {

	client := &gbClient{
		Name:  name,
		srv:   s,
		gbc:   conn,
		cType: clientType,
	}

	// Server lock here?
	s.numClientConnections++

	if initiated {
		client.directionType = INITIATED
		log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, name, clientType, conn.LocalAddr())
	} else {
		client.directionType = RECEIVED
		log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, name, clientType, conn.RemoteAddr())
	}

	// Read Loop for connection - reading and parsing off the wire and queueing to write if needed
	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.ServerName, fmt.Sprintf("read loop for %s", name), func() {
		defer conn.Close() // Should this be here if closure is managed elsewhere?
		client.readLoop()
	})

	//Write loop -

	return client

}

//===================================================================================
// Read Loop
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

	c.inbound.buffSize = INITIAL_BUFF_SIZE
	c.inbound.buffer = make([]byte, c.inbound.buffSize)
	buff := c.inbound.buffer

	reader := bufio.NewReader(c.gbc)

	// TODO: Look at back-pressure or flow control to prevent overwhelming the read loop

	//------------------
	//Beginning the read loop - read into buffer and adjust size if necessary - parse and process
	for {

		//c.cLock.Lock()

		n, err := reader.Read(buff)
		if err != nil {
			if err == io.EOF {
				log.Println("connection closed")
				return
			}
			log.Printf("read error: %s", err)
			return
		}

		// Check if we are utilizing more than half the buffer capacity - if not we may need to shrink
		if n <= cap(buff)/2 {
			log.Println("low buff utilization - increasing shrink count")
			c.inbound.expandCount++
		} else if n > cap(buff) {
			c.inbound.expandCount = 0
		}

		// Double buffer size if we reach capacity
		// TODO Look at a more efficient way of dynamically resizing

		if n >= cap(buff) && cap(buff) < MAX_BUFF_SIZE {
			c.inbound.buffSize = cap(buff) * 2
			if c.inbound.buffSize > MAX_BUFF_SIZE {
				c.inbound.buffSize = MAX_BUFF_SIZE
			}

			newBuff := make([]byte, c.inbound.buffSize)
			copy(newBuff, buff[:n])
			buff = newBuff
			log.Printf("increased buffer size to --> %d", len(buff))

		} else if n < cap(buff)/2 && cap(buff) > MIN_BUFF_SIZE && c.inbound.expandCount > 2 {
			c.inbound.buffSize = int(cap(buff) / 2)
			newBuff := make([]byte, c.inbound.buffSize)
			copy(newBuff, buff[:n])
			buff = newBuff
		}

		// Update offset
		c.inbound.offset = n

		//-----------------------------
		// Parsing the packet

		if c.cType == CLIENT {
			c.parsePacket(buff[:n])
		} else if c.cType == NODE {
			c.parsePacket(buff[:n])
		}

		log.Printf("bytes read --> %d", n)
		log.Printf("current buffer usage --> %d / %d", c.inbound.offset, len(buff))

		// TODO Think about flushing and writing and any clean up after the read

	}

}

//===================================================================================
// Write Loop, Queueing and Flushing
//===================================================================================

//---------------------------
//Queueing

// Lock should be held coming in to this
func (c *gbClient) queueOutbound(data []byte) {

	c.outbound.bytesInQ += uint64(len(data))
	log.Printf("number of bytes added to queue: %d for client %s", c.outbound.bytesInQ, c.gbc.RemoteAddr())

	// TODO Continue working on this ---

}

//---------------------------
//Write Loop

func (c *gbClient) writeLoop() {

	// Need to check if conn is closed and return
	// Need to check if flushed from last wake up

	for {
		c.outbound.outLock.Lock()

		c.flushWriteOutbound()
		c.outbound.outLock.Unlock()

	}
}

// ---------------------------
// Flushing
func (c *gbClient) flushWriteOutbound() {

}

//===================================================================================
// Handlers
//===================================================================================

func (c *gbClient) processINFO(arg []byte) error {
	// Assuming the first 3 bytes represent the command and the next bytes represent msgLength

	log.Println("printing arg from method")
	log.Println(arg)

	if len(arg) >= 4 {
		// Extract the last 4 bytes
		msgLengthBytes := arg[3:5]

		// Convert those 4 bytes to uint32 (BigEndian)
		c.nh.msgLength = int(binary.BigEndian.Uint16(msgLengthBytes))

		// Log the result to verify
		log.Printf("Extracted msgLength: %d\n", c.nh.msgLength)
	} else {
		return fmt.Errorf("argument does not have enough bytes to extract msgLength")
	}

	return nil
}
