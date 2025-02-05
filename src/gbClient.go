package src

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
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
	FLUSH_OUTBOUND
	CLOSED
	MARKED_CLOSED
	NO_RECONNECT
	WRITE_LOOP_STARTED
	READ_LOOP_STARTED
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

type ClosedState int

const (
	ClientClosed = ClosedState(iota + 1)
	WriteError
	ReadError
	ParseError
	ServerShutdown
)

//===================================================================================
// Client | Node
//===================================================================================

type gbClient struct {
	name    string
	created time.Time
	cid     uint64

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
	rh *responseHandler

	//Errors
	errChan chan error

	//Syncing
	mu       sync.Mutex
	refCount byte //Uint8
}

//===================================================================================
// Client Errors
//===================================================================================

type NodeReadError struct {
	err string
}

type NodeWriteError struct {
	err string
}

func (e *NodeReadError) Error() string {
	return fmt.Sprintf("read error: %s", e.err)
}

func (e *NodeWriteError) Error() string {
	return fmt.Sprintf("write error: %s", e.err)
}

// TODO separate the channels to avoid contention
func (c *gbClient) handleReadError(err error) {

	switch c.cType {
	case NODE:
		readErr := &NodeReadError{err: err.Error()}
		c.errChan <- readErr
		return
	case CLIENT:
		return
	}

}

func (c *gbClient) handleWriteError(err error) {

	switch c.cType {
	case NODE:
		writeErr := &NodeWriteError{err: err.Error()}
		c.errChan <- writeErr
		return
	case CLIENT:
		return
	}

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

	s := c.srv

	//Setup id for tracking in server map
	c.cid = atomic.AddUint64(&s.gcid, 1) // Assign unique ID
	//Outbound setup
	c.outbound.flushSignal = sync.NewCond(&(c.mu))

	//c.responseHandler.resp = make(map[int]chan []byte, 10) // Need to align with SeqID pool-size

	respHanlder := &responseHandler{}

	c.rh = respHanlder // Need to align with SeqID pool-size

	c.errChan = make(chan error, 1)

	return

}

// TODO Think about the locks we may need in this method
func (s *GBServer) createClient(conn net.Conn, name string, initiated bool, clientType int) *gbClient {

	client := &gbClient{
		name:  name,
		srv:   s,
		gbc:   conn,
		cType: clientType,
	}

	client.mu.Lock()

	tcp, err := net.ResolveTCPAddr(client.gbc.RemoteAddr().Network(), client.gbc.RemoteAddr().String())
	if err != nil {
		log.Printf("error resolving addr: %v", err)
	}

	client.tcpAddr = tcp

	client.initClient()

	client.mu.Unlock()

	//TODO:
	// At the moment - tmpClientStore is NEEDED in order to effectively close clients on server shutdown
	// This is important for fault detection as when a node/client goes down we won't know to close the connection unless
	// we detect it or us as a server shuts down
	//We also only get a read error once we close the connection - so we need to handle our connections in a robust way

	s.tmpConnStore.Store(client.cid, client)

	//TODO before starting the loops - handle TLS Handshake if needed
	// If TLS is needed - the client is a temporary 'unsafe' client until handshake complete or rejected

	// Read Loop for connection - reading and parsing off the wire and queueing to write if needed
	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.ServerName, fmt.Sprintf("read loop for %s", name), func() {
		defer conn.Close() // TODO Should this be here if closure is managed elsewhere?
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

func (s *GBServer) moveToConnected(cid uint64, name string) error {

	client, exists := s.tmpConnStore.LoadAndDelete(cid)
	if !exists {
		return fmt.Errorf("client %v not found", cid)
	}

	c, ok := client.(*gbClient)
	if !ok {
		return fmt.Errorf("client %v is not a client of type gbClient - got: %T", cid, client)
	}

	if _, exists := s.nodeConnStore.Load(name); exists {
		return fmt.Errorf("client %s already exists in nodeConnStore: %+v", name, c.gbc)
	}

	//TODO --> use server ID without timestamp to detect whether a node as restarted if so, check addr to verify and then
	// gossip digest to bring up to date, allow background node deleter to remove previous store when dead
	// - ! Need to remove old version of two of the same node !

	// If client not found we must check our cluster map for both server ID + Addr
	// If it's in there then we must decide on what to do - gossip and update - remove old entry

	switch c.cType {
	case NODE:
		s.nodeConnStore.Store(name, c) // TODO Moving to sync.Map so phase out nodeStore
		c.flags.set(CONNECTED)
	case CLIENT:
		s.clientStore[cid] = c
		c.flags.set(CONNECTED)
	}

	return nil

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
				//log.Printf("%s -- connection closed", c.srv.ServerName)
				// TODO need to do further check to see if our connection has dropped and implement reconnect strategy
				// Maybe it reaches out to another node?
				// Maybe it exits and then applies it's own reconnect with backoff retries
				// Will then need to log monitoring for full restart
				//buff = nil
				return
			}
			//log.Printf("%s -- read error: %s", c.srv.ServerName, err)
			//TODO Handle client closures more effectively - based on type
			// if client may want to reconnect and retry - if node we will want to use the phi accrual

			c.handleReadError(err)

			return
		}

		c.inbound.buffer = nil

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

		c.mu.Unlock()

		//-----------------------------
		// Parsing the packet

		if c.cType == CLIENT {
			c.parsePacket(buff[:n])
		} else if c.cType == NODE {
			c.parsePacket(buff[:n])
		}

		//log.Printf("%s -- read -- %s", c.srv.ServerName, buff[:n])
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

	if c.flags.isSet(FLUSH_OUTBOUND) {
		c.mu.Unlock()
		runtime.Gosched()
		c.mu.Lock()
		return false
	}

	c.flags.set(FLUSH_OUTBOUND)
	defer func() {
		c.flags.clear(FLUSH_OUTBOUND)
	}()

	if c.gbc == nil || c.srv == nil {
		return true
	}

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

	if len(c.outbound.writeBuffer) == 0 && cap(c.outbound.writeBuffer) > nbWritePoolLarge*8 {
		c.outbound.writeBuffer = nil
	}

	// Write errors
	if err != nil {
		c.handleWriteError(err)
		return true
	}

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
			//log.Printf("Waiting for flush signal... %s", c.srv.ServerName)
			//// Can I add a broadcast here instead
			c.outbound.flushSignal.Wait()
			//log.Println("Flush signal awakened.")
			if c.srv.serverContext.Err() != nil {
				//log.Printf("exiting write loop")
				return
			}
		}
		waitOk = c.flushWriteOutbound()

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

//===================================================================================
// Node Response Handling
//===================================================================================

// TODO Create standard response errors and protocol errors to return on the channel

type response struct {
	id      int
	ch      chan []byte
	timeout time.Duration
	err     chan error
}

type responseHandler struct {
	resp sync.Map
}

// Maybe resp needs to be an embedded struct of response {type, id, chan}

func (c *gbClient) addResponseChannel(seqID int) *response {

	rsp := &response{
		ch:  make(chan []byte, 1),
		err: make(chan error, 1),
		id:  seqID,
	}

	c.rh.resp.Store(seqID, rsp) // Store safely in sync.Map
	log.Printf("response channel made for %v", seqID)

	return rsp
}

// TODO Need to change cleanup in line with the new qProtoWithResponse - need to run in background and cleanup once channels are drained or timeout

func (c *gbClient) responseCleanup(rsp *response, respID uint16) {
	if val, ok := c.rh.resp.Load(int(respID)); ok {
		rsp := val.(*response)
		c.srv.releaseReqID(respID)
		close(rsp.ch)
		close(rsp.err)
		c.rh.resp.Delete(respID) // Remove safely

		log.Printf("responseCleanup - cleaned up response ID %d", respID)
	}
}

// TODO need to make generic wait for response that a caller can use to wait

func (c *gbClient) waitForResponse(ctx context.Context, rsp *response) ([]byte, error) {
	select {
	case <-ctx.Done():
		// Context canceled, return immediately
		log.Printf("waitForResponse - context canceled for response ID %d", rsp.id)
		return nil, ctx.Err()
	case writeErr := <-c.errChan:
		return nil, fmt.Errorf("write error: %w", writeErr)
	case readErr := <-c.errChan:
		return nil, fmt.Errorf("read error: %w", readErr)
	case msg := <-rsp.ch:
		log.Printf("waitForResponse - received response for ID %d: %s", rsp.id, msg)
		return msg, nil
	case err := <-rsp.err:
		return nil, fmt.Errorf("response error: %w", err)
	}
}

func (c *gbClient) waitForResponseAndBlock(ctx context.Context, rsp *response) ([]byte, error) {

	defer c.responseCleanup(rsp, uint16(rsp.id))

	resp, err := c.waitForResponse(ctx, rsp)
	if err != nil {
		return nil, err
	}

	return resp, nil

}

func (c *gbClient) waitForResponseAsync(ctx context.Context, rsp *response, handleResponse func([]byte, error)) {

	defer c.responseCleanup(rsp, uint16(rsp.id))

	go func() {
		log.Printf("waitForResponseAsync - waiting for response for ID %d", rsp.id)

		resp, err := c.waitForResponse(ctx, rsp)
		handleResponse(resp, err)

	}()

}

func (c *gbClient) getResponseChannel(id uint16) (*response, error) {

	if id == 0 {
		return nil, nil
	}

	responseChan, exists := c.rh.resp.Load(int(id))
	if !exists {
		return nil, fmt.Errorf("no response channel found for reqID %d", id)
	}

	// Type assertion to ensure we have the correct type
	rsp, ok := responseChan.(*response)
	if !ok {
		return nil, fmt.Errorf("invalid type assertion for response channel, reqID: %d", c.ph.reqID)
	}

	return rsp, nil

}

// Can think about inserting a command and callback function to specify what we want to do based on the response
// Lock not held on entry
func (c *gbClient) qProtoWithResponse(ctx context.Context, id uint16, proto []byte, flush bool, sendNow bool) *response {

	// TODO Do we need locks here
	c.mu.Lock()
	responseChannel := c.addResponseChannel(int(id))
	c.mu.Unlock()

	if sendNow && !flush {

		// Client lock to flush outbound
		c.mu.Lock()
		c.qProto(proto, false)
		c.flushWriteOutbound()
		c.mu.Unlock()

		// Wait for the response with timeout
		// We have to block and wait until we get a signal to continue the process which requested a response

	} else if sendNow {
		// Client lock to flush outbound
		c.mu.Lock()
		c.qProto(proto, true)
		//c.flushWriteOutbound()
		c.mu.Unlock()

		// Wait for the response with timeout
		// We have to block and wait until we get a signal to continue the process which requested a response
	}

	return responseChannel
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

func (c *gbClient) processMessage(message []byte) {

	//log.Printf("[DEBUG] %s Received message: %s", c.srv.ServerName, string(message))
	//log.Printf("[DEBUG] %s Processing message with ID: %d", c.srv.ServerName, c.ph.id)

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

func (s *GBServer) parseClientDelta(delta []byte, msgLen, keyLen, valueLen int) (int, error) {

	switch delta[0] {
	case 'V':

		s.clusterMapLock.Lock()
		defer s.clusterMapLock.Unlock()

		// TODO Need error checks here + correct locking

		key := delta[3 : 3+keyLen]

		value := delta[3+keyLen+1 : 2+keyLen+valueLen]

		now := time.Now().Unix()

		// Using self-info here because we are collecting the delta as part of our local state ready to be distributed
		newDelta := &Delta{
			key:       string(key),
			valueType: CLIENT_D,
			version:   now,
			value:     value,
		}

		dq := &deltaQueue{
			key:     newDelta.key,
			version: newDelta.version,
		}

		//s.selfInfo.valueIndex = append(s.selfInfo.valueIndex, string(key))
		s.selfInfo.keyValues[string(key)] = newDelta
		s.selfInfo.deltaQ.Push(&dq)

	}

	return 0, nil
}
