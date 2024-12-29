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
	outbound outboundQueue

	// cType determines if the conn is a node or client
	cType int
	// directionType determines if the conn was initiated (dialed by this server) or received (accepted in the accept loop)
	directionType string

	// TODO Add client options and better handling/separation of client types

	// Node client - extra node specific details
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

// TODO Should I have an inbound struct which has a pool and deals with processing inbound reads
//===================================================================================
// Outbound Node Write - For Per connection [Node] During Gossip Exchange
//===================================================================================

type outboundQueue struct {
	bytesInQ        int64
	writeBuffer     net.Buffers
	copyWriteBuffer net.Buffers
	flushSignal     *sync.Cond
	writeDuration   time.Duration
	flushTime       time.Duration
}

const (
	nbWritePoolSmall  = 512
	nbWritePoolMedium = 2048
	nbWritePoolLarge  = 4096
)

var nbPoolSmall = &sync.Pool{
	New: func() any {
		b := [nbWritePoolSmall]byte{}
		return &b
	},
}

var nbPoolMedium = &sync.Pool{
	New: func() any {
		b := [nbWritePoolMedium]byte{}
		return &b
	},
}

var nbPoolLarge = &sync.Pool{
	New: func() any {
		b := [nbWritePoolLarge]byte{}
		return &b
	},
}

func nbPoolGet(size int) []byte {
	switch {
	case size <= nbWritePoolSmall:
		return nbPoolSmall.Get().(*[nbWritePoolSmall]byte)[:0]
	case size <= nbWritePoolMedium:
		return nbPoolMedium.Get().(*[nbWritePoolMedium]byte)[:0]
	default:
		return nbPoolLarge.Get().(*[nbWritePoolLarge]byte)[:0]
	}
}

func nbPoolPut(b []byte) {
	switch cap(b) {
	case nbWritePoolSmall:
		b := (*[nbWritePoolSmall]byte)(b[0:nbWritePoolSmall])
		nbPoolSmall.Put(b)
	case nbWritePoolMedium:
		b := (*[nbWritePoolMedium]byte)(b[0:nbWritePoolMedium])
		nbPoolMedium.Put(b)
	case nbWritePoolLarge:
		b := (*[nbWritePoolLarge]byte)(b[0:nbWritePoolLarge])
		nbPoolLarge.Put(b)
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

	//TODO:
	// At the moment - tmpClientStore is NEEDED in order to effectively close clients on server shutdown
	// This is important for fault detection as when a node/client goes down we won't know to close the connection unless
	// we detect it or us as a server shuts down
	//We also only get a read error once we close the connection - so we need to handle our connections in a robust way
	s.tmpClientStore["1"] = client

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

// TODO Need to add more robust connection handling - including closures and reconnects

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

		n, err := reader.Read(buff)
		if err != nil {
			if err == io.EOF {
				log.Println("connection closed")
				return
			}
			log.Printf("read error: %s", err)
			return
		}

		c.mu.Lock()

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

		c.mu.Unlock()

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
		newPool := nbPoolGet(len(toBuffer))
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
		nbPoolPut(orig[i])
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

// To exit out of the wait loop gracefully with context - we need to wait on the context as a condition and once cancelled
// Broadcast will be called to signal all waiting go-routines to exit once the condition of context cancellation has been
// met
// https://pkg.go.dev/context#example-AfterFunc-Cond

func waitOnCond(ctx context.Context, cond *sync.Cond, conditionMet func() bool) error {

	stopCondition := context.AfterFunc(ctx, func() {

		cond.L.Lock()
		defer cond.L.Unlock()

		cond.Broadcast()

	})

	defer stopCondition()

	for !conditionMet() {
		cond.Wait()
		if ctx.Err() != nil {
			log.Printf("exiting write loop")
			return ctx.Err()
		}
	}

	return nil

}

func (c *gbClient) writeLoop() {

	c.mu.Lock()
	c.flags.set(WRITE_LOOP_STARTED)
	c.mu.Unlock()

	waitOk := true

	stopCondition := context.AfterFunc(c.srv.serverContext, func() {

		c.outbound.flushSignal.L.Lock()
		defer c.outbound.flushSignal.L.Unlock()

		c.outbound.flushSignal.Broadcast()

	})

	defer stopCondition()

	for {
		c.mu.Lock()
		if waitOk {
			//err := waitOnCond(c.srv.serverContext, c.outbound.flushSignal, func() bool {
			//	return !waitOk
			//})
			//if err != nil {
			//	c.mu.Unlock()
			//	return
			//}
			log.Printf("Waiting for flush signal... %s", c.srv.ServerName)
			//// Can I add a broadcast here instead
			c.outbound.flushSignal.Wait()
			log.Println("Flush signal awakened.")
			if c.srv.serverContext.Err() != nil {
				log.Printf("exiting write loop")
				return
			}
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

// Maybe resp needs to be an embedded struct of response {type, id, chan}

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
	case rsp := <-response:
		log.Printf("I got a response WOO!")
		log.Printf("response = %v", string(rsp))
		c.rm.Lock()
		delete(c.responseHandler.resp, int(respID))
		c.rm.Unlock()
		log.Printf("deleting response channel %d", int(respID))
		log.Printf("returning sequence to the pool")
		c.srv.releaseReqID(respID)
		close(response)
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
		c.mu.Lock()
		c.qProto(proto, false)
		c.flushWriteOutbound()
		c.mu.Unlock()

		// Wait for the response with timeout
		go c.waitForResponse(c.srv.serverContext, responseChannel, respID, 2*time.Second)
	}
}

//===================================================================================
// Parse Processors
//===================================================================================

func (c *gbClient) processDeltaHdr(arg []byte) error {

	c.ph.command = arg[0]
	msgLengthBytes := arg[1:3]
	c.ph.msgLength = int(binary.BigEndian.Uint16(msgLengthBytes))
	c.ph.keyLength = int(arg[3])
	valueLen := arg[4:6]
	c.ph.valueLength = int(binary.BigEndian.Uint16(valueLen))

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

	if c.cType == CLIENT {

		c.dispatchClientCommands(message)

	}
}

//===================================================================================
// Client Commands
//===================================================================================

func (c *gbClient) dispatchClientCommands(message []byte) {

	// Need a switch on commands
	switch c.ph.command {
	case 'V':
		log.Printf("command received: %v", string(c.ph.command))
		// Method for handling delta
		err := c.processDelta(message)
		if err != nil {
			log.Printf("error processing delta: %v", err)
		}
	}
}

// Delta handling method which will hand off to server to process in a go-routine
func (c *gbClient) processDelta(message []byte) error {

	srv := c.srv
	log.Printf("%s processing command", srv.ServerName)

	// Will need to copy message buf and msg length to avoid race conditions with the parser setting to nil
	// TODO Consider more efficient way of reducing allocations - maybe another pool of buffers? for inbound?
	// TODO Do testing to see if we need to copy here
	//msg := make([]byte, c.ph.msgLength)
	//copy(msg, message)
	msgLen := c.ph.msgLength

	keyLen := c.ph.keyLength
	valueLen := c.ph.valueLength

	// Can use server go-routine tracker ?? Or go func() to return an error
	// TODO Pass the header? Which should include the length, size, type and anything else needed)
	go func() {
		_, err := srv.parseClientDelta(message, msgLen, keyLen, valueLen)
		if err != nil {
			log.Printf("error parsing client delta message: %v", err)
		}
	}()

	return nil

}
