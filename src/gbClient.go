package src

import (
	"bufio"
	"context"
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

type clientFlags uint16 // Inspired by NATS bit mask for flags

// Flags
const (
	CONNECTED = 1 << iota
	GOSSIPING
	FLUSH_OUTBOUND
	CLOSED
	WRITE_LOOP_STARTED
	READ_LOOP_STARTED
	GOSS_SYN_SENT
	GOSS_SYN_REC
	GOSS_SYN_ACK_SENT
	GOSS_SYN_ACK_REC
	GOSS_ACK_SENT
	GOSS_ACK_REC
)

//goland:noinspection GoMixedReceiverTypes
func (cf *clientFlags) set(c clientFlags) {
	*cf |= c
}

//goland:noinspection GoMixedReceiverTypes
func (cf *clientFlags) clear(c clientFlags) {
	*cf &= ^c
}

//goland:noinspection GoMixedReceiverTypes
func (cf clientFlags) isSet(c clientFlags) bool {
	return cf&c != 0
}

//goland:noinspection GoMixedReceiverTypes
func (cf *clientFlags) setIfNotSet(c clientFlags) bool {
	if *cf&c == 0 {
		*cf |= c
		return true
	}
	return false
}

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

	// TODO Add client options and better handling/separation of client types

	// Node client
	node

	//Parsing + State
	stateMachine

	//Flags --> will tell us what state the client is in (connected, awaiting_syn_ack, etc...)
	flags clientFlags

	//Responses
	responseHandler

	//Syncing
	mu sync.Mutex
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
	bytesInQ        int64
	writeBuffer     net.Buffers
	copyWriteBuffer net.Buffers
	flushSignal     *sync.Cond
	writeDuration   time.Duration
	flushTime       time.Duration
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

func nodePoolPut(b []byte) {
	switch cap(b) {
	case NodeWritePoolSmall:
		b := (*[NodeWritePoolSmall]byte)(b[0:NodeWritePoolSmall])
		nodePoolSmall.Put(b)
	case NodeWritePoolMedium:
		b := (*[NodeWritePoolMedium]byte)(b[0:NodeWritePoolMedium])
		nodePoolMedium.Put(b)
	case NodeWritePoolLarge:
		b := (*[NodeWritePoolLarge]byte)(b[0:NodeWritePoolLarge])
		nodePoolLarge.Put(b)
	default:

	}
}

//===================================================================================
// Client creation
//===================================================================================

// TODO need init client with outbound data setup

func (c *gbClient) initClient() {

	//Outbound setup
	c.outbound.flushSignal = sync.NewCond(&(c.mu))

	c.responseHandler.resp = make(map[int]chan []byte, 10) // Need to align with SeqID pool-size

}

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

	client.mu.Lock()
	defer client.mu.Unlock()

	client.initClient()

	//TODO before starting the loops - handle TLS Handshake if needed
	// If TLS is needed - the client is a temporary 'unsafe' client until handshake complete or rejected

	// Read Loop for connection - reading and parsing off the wire and queueing to write if needed
	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.ServerName, fmt.Sprintf("read loop for %s", name), func() {
		defer conn.Close() // Should this be here if closure is managed elsewhere?
		client.readLoop()
	})

	//Write loop -
	s.startGoRoutine(s.ServerName, fmt.Sprintf("write loop for %s", name), func() {
		client.writeLoop()
	})

	if initiated {
		client.directionType = INITIATED
		//log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, name, clientType, conn.LocalAddr())
	} else {
		client.directionType = RECEIVED
		//log.Printf("%s logging received connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, name, clientType, conn.RemoteAddr())
	}

	return client

}

//===================================================================================
// Read Loop
//===================================================================================

//---------------------------
//Read Loop

func (c *gbClient) readLoop() {

	c.mu.Lock()

	if c.gbc == nil {
		defer c.mu.Unlock()
		c.flags.clear(READ_LOOP_STARTED)
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

	c.flags.set(READ_LOOP_STARTED)

	c.mu.Unlock()

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
			//log.Println("low buff utilization - increasing shrink count")
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
			//log.Printf("increased buffer size to --> %d", len(buff))

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

		//log.Printf("bytes read --> %d", n)
		//log.Printf("current buffer usage --> %d / %d", c.inbound.offset, len(buff))

		// TODO Think about flushing and writing and any clean up after the read

	}

}

//===================================================================================
// Write Loop, Queueing, Flushing and Response Handling
//===================================================================================

//---------------------------
//Queueing

// Lock should be held coming in to this
func (c *gbClient) queueOutbound(data []byte) {

	if c.gbc == nil {
		return
	}

	c.outbound.bytesInQ += int64(len(data))

	toBuffer := data

	// Topping up the queued buffer if it isn't full yet
	if len(c.outbound.writeBuffer) > 0 {
		last := &c.outbound.writeBuffer[len(c.outbound.writeBuffer)-1]
		if free := cap(*last) - len(*last); free > 0 {
			if l := len(toBuffer); l < free {
				free = l
			}
			*last = append(*last, toBuffer[:free]...)
			toBuffer = toBuffer[free:]
		}
	}

	for len(toBuffer) > 0 {
		newPool := nodePoolGet(len(toBuffer))
		n := copy(newPool[:cap(newPool)], toBuffer)
		c.outbound.writeBuffer = append(c.outbound.writeBuffer, newPool[:n])
		toBuffer = toBuffer[n:]
	}

	// Buffer pool will be returned when we flush outbound

}

// ---------------------------
// Flushing

// Lock must be held
func (c *gbClient) flushSignal() {
	if c.outbound.flushSignal != nil {
		c.outbound.flushSignal.Signal()
	}
}

// Lock must be held coming in
func (c *gbClient) flushWriteOutbound() bool {

	c.flags.set(FLUSH_OUTBOUND)
	defer func() {
		c.flags.clear(FLUSH_OUTBOUND)
	}()

	toWrite := c.outbound.writeBuffer
	c.outbound.writeBuffer = nil

	nc := c.gbc

	c.mu.Unlock()

	c.outbound.copyWriteBuffer = append(c.outbound.copyWriteBuffer, toWrite...)

	var _orig [1024][]byte
	orig := append(_orig[:0], c.outbound.copyWriteBuffer...)

	start := c.outbound.copyWriteBuffer[0:]

	var n int64
	var wn int64
	var err error

	for len(c.outbound.copyWriteBuffer) > 0 {

		wb := c.outbound.copyWriteBuffer
		if len(wb) > 1024 {
			wb = wb[:1024]
		}
		consumed := len(wb)

		wn, err = wb.WriteTo(nc)

		n += wn
		c.outbound.copyWriteBuffer = c.outbound.copyWriteBuffer[consumed-len(wb):]
		if err != nil && err != io.ErrShortWrite {
			break
		}
	}

	c.mu.Lock()

	for i := 0; i < len(orig)-len(c.outbound.copyWriteBuffer); i++ {
		nodePoolPut(orig[i])
	}

	c.outbound.copyWriteBuffer = append(start[:0], c.outbound.copyWriteBuffer...)

	c.outbound.bytesInQ -= n

	if c.outbound.bytesInQ > 0 {
		c.flushSignal()
	}

	return true

}

//---------------------------
//Write Loop

//TODO sync.Cond requires that the associated lock be held when calling Wait and Signal.
// releases the lock temporarily while waiting and reacquires it before returning.

// TODO Need to add context control and errors to write loop

func (c *gbClient) writeLoop() {

	c.mu.Lock()
	c.flags.set(WRITE_LOOP_STARTED)
	c.mu.Unlock()

	waitOk := true

	for {
		c.mu.Lock()

		if waitOk {
			log.Printf("Waiting for flush signal... %s", c.srv.ServerName)
			// Can I add a broadcast here instead
			c.outbound.flushSignal.Wait()
			log.Println("Flush signal awakened.")
		}
		waitOk = c.flushWriteOutbound()
		log.Printf("flushWriteOutbound result: %v", waitOk)

		c.mu.Unlock()

	}
}

//--------------------------
// Queue Proto

// Lock should be held
func (c *gbClient) qProto(proto []byte, flush bool) {
	c.queueOutbound(proto)
	if c.outbound.flushSignal != nil && flush {
		c.outbound.flushSignal.Signal()
	}
}

//---------------------------
//Node Response Handling

type responseHandler struct {
	resp    map[int]chan []byte
	timeout time.Duration
	rm      sync.Mutex
}

func (c *gbClient) addResponseChannel(seqID int) chan []byte {

	respChan := make(chan []byte, 1)

	log.Printf("adding response channel %d", seqID)

	c.rm.Lock()
	c.resp[seqID] = respChan
	c.rm.Unlock()

	return respChan
}

func (c *gbClient) waitForResponse(ctx context.Context, response chan []byte, respID byte, timeout time.Duration) {

	// Wait for the response with timeout
	select {
	case <-ctx.Done():
		log.Printf("context cancelled waiting for response channel %d", respID)
		close(response)
		c.rm.Lock()
		delete(c.responseHandler.resp, int(respID))
		c.rm.Unlock()
		log.Printf("deleting response channel %d", int(respID))
		log.Printf("returning sequence to the pool")
		c.srv.releaseReqID(respID)
		return
	case response := <-response:
		log.Printf("I got a response WOO!")
		log.Printf("response = %v", string(response))
		c.rm.Lock()
		delete(c.responseHandler.resp, int(respID))
		c.rm.Unlock()
		log.Printf("deleting response channel %d", int(respID))
		log.Printf("returning sequence to the pool")
		c.srv.releaseReqID(respID)
		return
	case <-time.After(timeout):
		// Clean up the response channel on timeout
		close(response)
		c.rm.Lock()
		delete(c.responseHandler.resp, int(respID))
		c.rm.Unlock()
		log.Printf("timed out waiting for response channel %d", int(respID))
		return
	}

}

// Lock not held on entry
func (c *gbClient) qProtoWithResponse(proto []byte, flush bool, sendNow bool) {

	respID := proto[2]

	responseChannel := c.addResponseChannel(int(respID))

	if sendNow {

		// Client lock to flush outbound
		c.qProto(proto, false)
		c.mu.Lock()
		c.flushWriteOutbound()
		c.mu.Unlock()
	}

	// Wait for the response with timeout
	go c.waitForResponse(c.srv.serverContext, responseChannel, respID, 2*time.Second)

}

//===================================================================================
// Handlers
//===================================================================================

func (c *gbClient) processINFO(arg []byte) error {
	// Assuming the first 3 bytes represent the command and the next bytes represent msgLength

	if len(arg) >= 4 {
		c.nh.version = arg[0]
		c.nh.id = arg[2]
		c.nh.command = arg[1]
		// Extract the last 4 bytes
		msgLengthBytes := arg[3:5]
		// Convert those 4 bytes to uint32 (BigEndian)
		c.nh.msgLength = int(binary.BigEndian.Uint16(msgLengthBytes))

		// Log the result to verify
		//log.Printf("Extracted msgLength: %d\n", c.nh.msgLength)
	} else {
		return fmt.Errorf("argument does not have enough bytes to extract msgLength")
	}

	c.argBuf = arg

	return nil
}

//===================================================================================
// Dispatcher
//===================================================================================

//TODO Need a process message + command dispatcher
// use switch case for client type
// if node - use switch case for command type

func (c *gbClient) processMessage(message []byte) {
	if c.cType == NODE {

		c.dispatchNodeCommands(message)

	}
}

//---------------------------
// Node Handlers

func (c *gbClient) dispatchNodeCommands(message []byte) {

	//GOSS_SYN
	//GOSS_SYN_ACK
	//GOSS_ACK
	//TEST

	switch c.nh.command {
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
		log.Printf("unknown command %v", c.nh.command)
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

		c.mu.Lock()
		delete(c.resp, int(c.argBuf[0]))
		c.mu.Unlock()

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
	header := constructNodeHeader(1, OK, 1, uint16(len(cereal)), NODE_HEADER_SIZE_V1)
	// Create packet
	packet := &nodePacket{
		header,
		cereal,
	}
	pay1, _ := packet.serialize()

	c.qProto(pay1, true)

}
