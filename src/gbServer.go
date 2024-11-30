package src

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

//TODO -- STEPS FOR TCP ACCEPT LOOP SERVER CONTROL
// - 2) Server Context and signals to control the server instance

//=================================

//TODO -- 151124 !! -- Now server node can connect to seed will need -->
// 1) Server info and metrics and connection map
// 2) Signals for syncing
// 3) Seed server needs to reply with its info and make connection
// 4) Protocol reading and parsing

// CONSIDERATIONS //
// - TCP Keep-Alives default to no less than two hours which means that using anti-entropy with direct and in-direct
//		heartbeats along with gossip exchanges, the TCP connection will be kept alive unless detected otherwise by the
//		algorithm
//------------------------

// Maybe want to declare some global const names for contexts -- seedServerContext -- nodeContext etc

//===================================================================================
// Main Server
//===================================================================================

type GBServer struct {
	//Server Info - can add separate info struct later
	ServerName    string //Set by config or flags
	BroadcastName string //ID and timestamp
	initialised   int64  //time of server creation
	addr          string
	nodeTCPAddr   *net.TCPAddr
	clientTCPAddr *net.TCPAddr

	// Metrics or values for gossip
	// CPU Load
	// Latency ...
	//

	listener           net.Listener
	listenConfig       net.ListenConfig
	nodeListener       net.Listener
	nodeListenerConfig net.ListenConfig

	//Context
	serverContext       context.Context
	serverContextCancel context.CancelFunc
	shuttingDown        sync.Map

	gbConfig *GbConfig
	seedAddr []*net.TCPAddr

	//Server Info for gossip
	selfInfo   *Participant
	clusterMap ClusterMap

	//Distributed Info
	isOriginal  bool
	isGossiping chan bool

	//Connection Handling
	tmpClientStore       map[string]*gbClient
	numNodeConnections   uint8
	numClientConnections uint16

	//phoneBook      map[string]*gbClient

	//connMutex      sync.RWMutex

	//pool           sync.Pool // Maybe to use with varying buffer sizes

	// nodeReqPool is for the server when acting as a client/node initiating requests of other nodes
	//it must maintain a pool of active sequence numbers for open requests awaiting response
	nodeReqPool seqReqPool

	serverLock sync.RWMutex

	//serverWg *sync.WaitGroup

	//go-routine tracking
	grTracking
}

//TODO Add a node listener config + also client and node addr

func NewServer(serverName string, gbConfig *GbConfig, nodeHost string, nodePort, clientPort string, lc net.ListenConfig) *GBServer {

	addr := net.JoinHostPort(nodeHost, nodePort)
	nodeTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	cAddr := net.JoinHostPort("0.0.0.0", clientPort)

	clientAddr, err := net.ResolveTCPAddr("tcp", cAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Creation steps
	// Gather server metrics

	createdAt := time.Now()

	broadCastName := fmt.Sprintf("%s_%s", serverName, createdAt.Format("20060102150405"))

	seq := newSeqReqPool(10)

	ctx, cancel := context.WithCancel(context.Background())

	s := &GBServer{
		ServerName:          serverName,
		BroadcastName:       broadCastName,
		initialised:         createdAt.Unix(),
		addr:                addr,
		nodeTCPAddr:         nodeTCPAddr,
		clientTCPAddr:       clientAddr,
		listenConfig:        lc,
		serverContext:       ctx,
		serverContextCancel: cancel,

		gbConfig:       gbConfig,
		seedAddr:       make([]*net.TCPAddr, 0),
		tmpClientStore: make(map[string]*gbClient),

		isOriginal:           false,
		numNodeConnections:   0,
		numClientConnections: 0,

		nodeReqPool: *seq,

		// TODO Create init method and point to it here on server initialisation
		grTracking: grTracking{
			index:       0,
			numRoutines: 0,
			grWg:        &sync.WaitGroup{},
		},
	}

	return s
}

// Start server will be a go routine alongside this, the server will have to perform connection dials to other servers
// to maintain persistent connections and perform reconciliation of cluster map

func (s *GBServer) StartServer() {

	fmt.Printf("Server starting: %s\n", s.ServerName)
	fmt.Printf("Server address: %s, Seed address: %v\n", s.nodeTCPAddr, s.gbConfig.SeedServers)

	//Checks and other start up here
	//Resolve config seed addr
	err := s.resolveConfigSeedAddr()
	if err != nil {
		log.Fatal(err)
	}

	// This needs to be a method with locks

	// Move this seed logic elsewhere
	// TODO if we are not seed then we need to reach out - set a flag for this (initiator)
	if s.seedCheck() == 1 {
		s.isOriginal = true
	} else {
		s.isOriginal = false
	}

	// Setting go routine tracking flag to true - mainly used in testing
	s.grTracking.trackingFlag.Store(true)

	//---------------- Node Accept Loop ----------------//
	s.AcceptNodeLoop("node-test")

	//---------------- Client Accept Loop ----------------//
	s.AcceptLoop("client-test")

	fmt.Printf("%s %v\n", s.ServerName, s.isOriginal)
}

//=======================================================

func (s *GBServer) Shutdown() {
	log.Printf("%s -- shut down initiated\n", s.ServerName)
	s.shuttingDown.Store("shutdown", true)

	log.Println("context called")
	s.serverContextCancel()

	if s.listener != nil {
		log.Println("closing client listener")
		s.listener.Close()
		s.listener = nil
	}
	if s.nodeListener != nil {
		log.Println("closing node listener")
		s.nodeListener.Close()
		s.nodeListener = nil
	}

	//Close connections
	for name, client := range s.tmpClientStore {
		log.Printf("closing client %s\n", name)
		client.gbc.Close()
		delete(s.tmpClientStore, name)
	}

	log.Println("waiting...")
	s.grWg.Wait()
	log.Println("done")

	log.Println("Server shutdown complete")
}

//=======================================================

//=======================================================

func (s *GBServer) resolveConfigSeedAddr() error {

	// Check if no seed servers are configured
	if s.gbConfig.SeedServers == nil {
		// Ensure s.nodeTCPAddr is initialized
		if s.nodeTCPAddr == nil {
			return fmt.Errorf("nodeTCPAddr is not initialized")
		}
		// Use this node's TCP address as the seed
		s.serverLock.Lock()
		s.seedAddr = append(s.seedAddr, s.nodeTCPAddr)
		s.serverLock.Unlock()
		//log.Printf("seed server list --> %v\n", s.seedAddr)
		return nil
	}

	if len(s.gbConfig.SeedServers) >= 1 {

		s.serverLock.Lock()
		defer s.serverLock.Unlock()

		for i := 0; i < len(s.gbConfig.SeedServers); i++ {
			addr := net.JoinHostPort(s.gbConfig.SeedServers[i].SeedIP, s.gbConfig.SeedServers[i].SeedPort)
			tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
			if err != nil {
				return err
			}
			s.seedAddr = append(s.seedAddr, tcpAddr)
			//log.Printf("seed server list --> %v\n", s.seedAddr)
		}
	}
	return nil
}

// Seed Check

func (s *GBServer) seedCheck() int {

	if len(s.seedAddr) >= 1 {
		for _, addr := range s.seedAddr {
			if addr.IP.Equal(s.nodeTCPAddr.IP) && addr.Port == s.nodeTCPAddr.Port {
				return 1
			}
		}
	}

	return 0

}

//=======================================================
// Accept Loops
//=======================================================

func (s *GBServer) AcceptLoop(name string) {

	ctx, cancel := context.WithCancel(s.serverContext)
	defer cancel() //TODO Need to think about context cancel for connection handling and retry logic/client disconnect

	log.Printf("Starting client accept loop -- %s\n", name)

	log.Printf("Creating client listener on %s\n", s.clientTCPAddr.String())

	l, err := s.listenConfig.Listen(s.serverContext, s.clientTCPAddr.Network(), s.clientTCPAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	// Add listener to the server
	s.listener = l

	// Can begin go-routine for accepting connections
	go s.acceptConnection(l, "client-test",
		func(conn net.Conn) {
			s.createClient(conn, "normal-client", false, CLIENT)
		},
		func(err error) bool {
			select {
			case <-ctx.Done():
				//log.Println("accept loop context canceled -- exiting loop")
				return true
			default:
				//log.Printf("accept loop context error -- %s\n", err)
				return false
			}
		})
}

//TODO Figure out how to manage routines and shutdown signals

func (s *GBServer) AcceptNodeLoop(name string) {

	ctx, cancel := context.WithCancel(s.serverContext)
	defer cancel() //TODO Need to think about context cancel for connection handling and retry logic/node disconnect

	log.Printf("Starting node accept loop -- %s\n", name)

	log.Printf("Creating node listener on %s\n", s.nodeTCPAddr.String())

	nl, err := s.listenConfig.Listen(s.serverContext, s.nodeTCPAddr.Network(), s.nodeTCPAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	s.nodeListener = nl

	//go s.acceptConnection(nl, "node-test", func(conn net.Conn) { s.createNodeClient(conn, "node-client", false, NODE) })
	s.startGoRoutine(s.ServerName, "accept-connection routine", func() {
		s.acceptConnection(nl, "node-test",
			func(conn net.Conn) {
				s.createNodeClient(conn, "node-client", false, NODE)
			},
			func(err error) bool {
				select {
				case <-ctx.Done():
					log.Println("accept loop context canceled -- exiting loop")
					return true
				default:
					log.Printf("accept loop context error -- %s\n", err)
					return false
				}
			})
	})

	//---------------- Seed Dial ----------------//
	//This is essentially a solicit
	if !s.isOriginal {
		// If we're not the original (seed) node, connect to the seed server
		//go s.connectToSeed()
		s.startGoRoutine(s.ServerName, "connect to seed routine", func() {
			s.connectToSeed()
		})
	}

}

//=======================================================
// Accept Connection - taking connections from different listeners
//=======================================================

// TODO add a callback error function for any read errors to signal client closures?
// TODO consider adding client connection scoped context...?

func (s *GBServer) acceptConnection(l net.Listener, name string, createConnFunc func(conn net.Conn), customErr func(err error) bool) {

	delayCount := int(3)
	tmpDelay := int(0)

	for {
		conn, err := l.Accept()
		if err != nil {
			if customErr != nil && customErr(err) {
				log.Println("custom error called")
				return // we break here to come out of the loop - if we can't reconnect during a reconnect strategy then we break
			}
			if tmpDelay < delayCount {
				log.Println("retry ", tmpDelay)
				tmpDelay++
				time.Sleep(1 * time.Second)
				continue
			} else {
				log.Println("retry limit")
				break
			}
			continue
		}
		// go createFunc(conn)
		s.startGoRoutine(s.ServerName, "create connection routine", func() {
			createConnFunc(conn)
		})
	}

	log.Println("accept loop exited")

}

//=======================================================
// Creating a node server
//=======================================================

//==

//=======================================================
// Sync Pool for Server-Server Request cycles
//=======================================================

type seqReqPool struct {
	reqPool *sync.Pool
}

func newSeqReqPool(poolSize uint8) *seqReqPool {
	if poolSize > 255 {
		poolSize = 255
	} else if poolSize == 0 {
		poolSize = 10
	}

	sequence := make(chan uint8, poolSize)
	for i := 1; i < int(poolSize)+1; i++ {
		sequence <- uint8(i)
	}

	return &seqReqPool{
		reqPool: &sync.Pool{
			New: func() any {
				select {
				case id := <-sequence:
					log.Printf("Allocating sequence ID %d from the pool", id) // Log allocations
					return id
				default:
					log.Println("Pool exhausted: no sequence IDs available")
					return nil
				}
			},
		},
	}
}

func (s *GBServer) acquireReqID() (uint8, error) {
	id := s.nodeReqPool.reqPool.Get()
	if id == nil {
		return 0, fmt.Errorf("no id available")
	}
	return id.(uint8), nil
}

func (s *GBServer) releaseReqID(id uint8) {
	log.Printf("Releasing sequence ID %d back to the pool", id)
	s.nodeReqPool.reqPool.Put(id)
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

	//Server Lock?
	s.numNodeConnections++

	// Only log if the connection was initiated by this server (to avoid duplicate logs)
	if initiated {
		client.directionType = INITIATED
		log.Printf("%s logging initiated connection --> %s --> type: %d --> conn addr %s\n", s.ServerName, client.Name, clientType, conn.LocalAddr())
		// TODO if the client initiated the connection and is a new NODE then it must send info on first message

		client.queueOutbound([]byte("hello"))

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

	// TODO Add this to outbound queue instead and let flush outbound handle the write
	_, err = conn.Write(pay1)
	if err != nil {
		return fmt.Errorf("error writing to connection: %s", err)
	}

	//Once it has successfully dialled we want to create a node client and store the connection
	// + wait for info exchange

	s.createNodeClient(conn, "whaaaat", true, NODE)

	select {
	case <-ctx.Done():
		log.Println("connect to seed cancelled because of context")
		s.releaseReqID(seq)
	}

	return nil

}

//=======================================================
// Node Info + Initial Connect Packet Creation
//=======================================================

func (s *GBServer) initSelfParticipant() {

	t := time.Now().Unix()

	p := &Participant{
		name:       s.ServerName,
		keyValues:  make(map[int]*Delta),
		maxVersion: -1,
	}

	p.keyValues[ADDR_V] = &Delta{
		valueType: STRING_DV,
		version:   t,
		value:     []byte(s.addr),
	}
	// Set the numNodeConnections delta
	numNodeConnBytes := make([]byte, 1)
	numNodeConnBytes[0] = s.numNodeConnections
	p.keyValues[NUM_NODE_CONN_V] = &Delta{
		valueType: INT_DV,
		version:   t,
		value:     numNodeConnBytes,
	}

	// TODO need to figure how to update maxVersion

	s.serverLock.Lock()
	s.selfInfo = p
	s.serverLock.Unlock()

}

// TODO Think about how to keep the internal state up to date for gossiping

// TODO Will need data lengths prefixed to messages which are not part of header in order to parse the message payload
