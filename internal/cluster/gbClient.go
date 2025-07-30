package cluster

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	uuid2 "github.com/google/uuid"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"io"
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
	//stateMachine
	parser

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

func (s *GBServer) createClient(conn net.Conn, clientType int) *gbClient {

	uuid := uuid2.New()
	now := time.Now()
	clientName := fmt.Sprintf("%s:%d", uuid, now.Unix())

	s.logger.Info("creating client connection", "address", conn.LocalAddr().String())

	client := &gbClient{
		name:  clientName,
		srv:   s,
		gbc:   conn,
		cType: clientType,
	}

	client.mu.Lock()
	client.initClient()
	client.mu.Unlock()

	s.clientStore.Store(clientName, client)

	// Read Loop for connection - reading and parsing off the wire and queueing to write if needed
	// Track the goroutine for the read loop using startGoRoutine
	s.startGoRoutine(s.PrettyName(), fmt.Sprintf("read loop for %s", clientName), func() {
		client.readLoop()
	})

	//Write loop -
	s.startGoRoutine(s.PrettyName(), fmt.Sprintf("write loop for %s", clientName), func() {
		client.writeLoop()
	})

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

	switch c.cType {
	case NODE:
		s.nodeConnStore.Store(name, c)
		c.flags.set(CONNECTED)
	case CLIENT:
		return fmt.Errorf("found client in tmpConnStore %s", name)
	default:
		return nil
	}

	return nil

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

	//------------------
	//Beginning the read loop - read into buffer and adjust size if necessary - parse and process

	c.flags.set(READ_LOOP_STARTED)

	c.mu.Unlock()

	for {

		n, err := reader.Read(buff)
		if err != nil {
			if err == io.EOF {
				// Maybe it reaches out to another node?
				// Maybe it exits and then applies its own reconnect with backoff retries
				// Will then need to log monitoring for full restart
				//buff = nil
				return
			}

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

		if n >= cap(buff) && cap(buff) < MAX_BUFF_SIZE {
			c.inbound.buffSize = cap(buff) * 2
			if c.inbound.buffSize > MAX_BUFF_SIZE {
				c.inbound.buffSize = MAX_BUFF_SIZE
			}

			newBuff := make([]byte, c.inbound.buffSize)
			copy(newBuff, buff[:n])
			buff = newBuff

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
			c.ParsePacket(buff[:n])
		} else if c.cType == NODE {
			c.ParsePacket(buff[:n])
		}

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
		fmt.Printf("flushWriteOutbound: Write error: %v\n", err)
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

// To exit out of the wait loop gracefully with context - we need to wait on the context as a condition and once cancelled
// Broadcast will be called to signal all waiting go-routines to exit once the condition of context cancellation has been
// met
// https://pkg.go.dev/context#example-AfterFunc-Cond

func (c *gbClient) writeLoop() {

	c.mu.Lock()
	c.flags.set(WRITE_LOOP_STARTED)
	c.mu.Unlock()

	waitOk := true

	stopCondition := context.AfterFunc(c.srv.ServerContext, func() {

		c.outbound.flushSignal.L.Lock()
		defer c.outbound.flushSignal.L.Unlock()

		c.outbound.flushSignal.Broadcast()

	})

	defer stopCondition()

	for {
		c.mu.Lock()
		if waitOk {
			//// Can I add a broadcast here instead
			c.outbound.flushSignal.Wait()
			if c.srv.ServerContext.Err() != nil {
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

func (c *gbClient) enqueueProtoAndFlush(data []byte, doFlush bool) {
	if c.gbc == nil {
		return
	}

	c.queueOutbound(data) // Queue the data

	// Attempt to flush if requested, otherwise signal the write loop
	if !(doFlush && c.flushWriteOutbound()) {
		c.flushSignal()
	}
}

func (c *gbClient) sendProtoNow(data []byte) {
	c.enqueueProtoAndFlush(data, true) // Flush immediately
}

func (c *gbClient) enqueueProto(data []byte) {
	c.enqueueProtoAndFlush(data, false) // Just queue and signal the write loop
}

//===================================================================================
// Response Handling
//===================================================================================

type responsePayload struct {
	respID uint16
	reqID  uint16
	msg    []byte
}

type response struct {
	id      int
	ch      chan responsePayload
	timeout time.Duration
	err     chan error
	ctx     context.Context
}

type responseHandler struct {
	resp sync.Map
}

func (c *gbClient) addResponseChannel(ctx context.Context, seqID int) *response {

	rsp := &response{
		ch:  make(chan responsePayload, 1),
		err: make(chan error, 1),
		id:  seqID,
		ctx: ctx,
	}

	c.rh.resp.Store(seqID, rsp) // Store safely in sync.Map
	//log.Printf("response channel made for %v", seqID)

	return rsp
}

func (c *gbClient) responseCleanup(rsp *response, respID uint16) {
	if val, ok := c.rh.resp.Load(int(respID)); ok {
		_ = val.(*response)
		c.srv.releaseReqID(respID)
		//close(rsp.ch)
		//close(rsp.err)
		c.rh.resp.Delete(respID) // Remove safely

		//log.Printf("responseCleanup - cleaned up response ID %d", respID)
	}
}

func (c *gbClient) waitForResponse(rsp *response) (responsePayload, error) {
	select {
	case <-rsp.ctx.Done():
		return responsePayload{}, fmt.Errorf("response timeout: %w", rsp.ctx.Err())

	case msg := <-rsp.ch:
		return msg, nil

	case err := <-rsp.err:
		return responsePayload{}, Errors.ChainGBErrorf(Errors.ResponseErr, err, "")
	}
}

func (c *gbClient) waitForResponseAndBlock(rsp *response) (responsePayload, error) {

	defer c.responseCleanup(rsp, uint16(rsp.id))

	resp, err := c.waitForResponse(rsp)
	if err != nil {
		return responsePayload{}, err
	}

	return resp, nil

}

// Must defer response cleanup in callback
func (c *gbClient) waitForResponseAsync(rsp *response, handleResponse func(responsePayload, error)) {

	go func() {

		defer c.responseCleanup(rsp, uint16(rsp.id))

		resp, err := c.waitForResponse(rsp)

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
func (c *gbClient) qProtoWithResponse(ctx context.Context, id uint16, proto []byte, sendNow bool) *response {
	rsp := c.addResponseChannel(ctx, int(id)) // Create a response channel

	c.mu.Lock()
	c.enqueueProto(proto) // Use the new enqueue method
	c.mu.Unlock()

	if sendNow {
		c.mu.Lock()
		c.flushWriteOutbound() // Force immediate flush
		c.mu.Unlock()
	}

	return rsp // Return response channel so caller can wait for it
}

//===================================================================================
// Parse Processors
//===================================================================================

func (c *gbClient) processDeltaHdr(arg []byte) error {

	c.ph.command = arg[0]
	msgLengthBytes := arg[1:3]
	c.ph.msgLength = binary.BigEndian.Uint16(msgLengthBytes)
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

// TODO Add command list here and regex on command for parsing and failing early

// Client commands

const (
	PING         = "PING"
	SNAPSHOT_LOG = "SNAPSHOT LOG"
	STREAM_LOG   = "STREAM_LOGS"
	GET_KEYS     = "GET KEYS"
	MAX_VERSION  = "MAX VERSION"
	NODES        = "NODES"
)

const (
	PING_ENUM        = uint8(iota + 1)
	SNAPSHOT_ENUM    = 2
	STREAM_ENUM      = 3
	GET_KEYS_ENUM    = 4
	MAX_VERSION_ENUM = 5
	NODES_ENUM       = 6
)

func parseClientCommandToCommandEnum(clientCommand string) uint8 {

	switch clientCommand {
	case PING:
		return PING_ENUM
	case SNAPSHOT_LOG:
		return SNAPSHOT_ENUM
	case STREAM_LOG:
		return STREAM_ENUM
	case GET_KEYS:
		return GET_KEYS_ENUM
	case MAX_VERSION:
		return MAX_VERSION_ENUM
	case NODES:
		return NODES_ENUM
	default:
		return 0
	}

}

func (c *gbClient) dispatchClientCommands(message []byte) {

	command := parseClientCommandToCommandEnum(string(message))

	// Need a switch on commands
	switch command {
	//e.g
	//case STREAM_LOG ('stream log'):
	// --->
	case uint8(0):
		c.unrecognisedCommand(message)
	case PING_ENUM:
		c.handlePING()
	case STREAM_ENUM:
		c.streamLogs()
	}
}

func (c *gbClient) unrecognisedCommand(command []byte) {

	resp := []byte(fmt.Sprintf("unrecognised command --> %s\n", command))

	c.mu.Lock()
	c.enqueueProto(resp)
	c.mu.Unlock()

}

func (c *gbClient) handlePING() {

	resp := []byte("PONG\r\n")

	c.mu.Lock()
	c.enqueueProto(resp)
	c.mu.Unlock()

	return

}

func (c *gbClient) streamLogs() {

	c.srv.slogHandler.AddStreamLoggerHandler(
		c.srv.ServerContext,
		c.name,
		func(b []byte) error {
			// Append CRLF without an extra allocation when possible.
			msg := append(b, '\r', '\n')

			c.mu.Lock()
			c.enqueueProto(msg)
			c.mu.Unlock()

			return nil
		},
	)

}

func (c *gbClient) stopStreaming() {

	// finds the handler and closes it?

}
