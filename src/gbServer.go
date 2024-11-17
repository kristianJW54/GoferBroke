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
// - 5) Add comprehensive wrappers for go routine control and tracking

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

type Config struct {
	Seed *net.TCPAddr
} //Temp will be moved

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

	//TCP - May want to abstract or package this elsewhere and let the server hold that package to conduct it's networking...?
	// Network package here?? which can hold persistent connections?
	// We can give an interface here which we can pass in mocks or different methods with controls and configs?
	listener     net.Listener
	listenConfig net.ListenConfig

	//Context
	serverContext       context.Context
	serverContextCancel context.CancelFunc
	shuttingDown        bool

	acceptLoopContext context.Context

	gbConfig *GbConfig
	seedAddr []*net.TCPAddr

	//Distributed Info
	isOriginal  bool
	isGossiping chan bool

	nodeListener       net.Listener
	nodeListenerConfig net.ListenConfig

	//Connection Handling
	clientCount    int
	tmpClientStore map[string]*gbClient
	phoneBook      map[string]*gbClient
	connMutex      sync.RWMutex
	pool           sync.Pool // Maybe to use with varying buffer sizes

	cRM sync.RWMutex

	quitCtx chan struct{}
	done    chan bool
	ready   chan struct{}

	serverWg *sync.WaitGroup

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

	createdAt := time.Now()

	broadCastName := fmt.Sprintf("%s_%s", serverName, createdAt.Format("20060102150405"))

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
		shuttingDown:        false,

		gbConfig:       gbConfig,
		seedAddr:       make([]*net.TCPAddr, 0),
		tmpClientStore: make(map[string]*gbClient),

		isOriginal: false,

		quitCtx: make(chan struct{}, 1),
		done:    make(chan bool, 1),
		ready:   make(chan struct{}, 1),

		serverWg: &sync.WaitGroup{},

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

	//---------------- Node Accept Loop ----------------//
	//s.serverWg.Add(1)
	//go func() {
	//	defer s.serverWg.Done()
	//	s.AcceptNodeLoop("node-test") // Wrap this and put in go-routine
	//}()

	s.grTracking.trackingFlag.Store(true)

	s.AcceptNodeLoop("node-test")

	//---------------- Client Accept Loop ----------------//
	//s.AcceptLoop("client-test")

	fmt.Printf("%s %v\n", s.ServerName, s.isOriginal)
}

//=======================================================

func (s *GBServer) Shutdown() {
	s.serverContextCancel()
	log.Println("Waiting for all connections to close...")

	s.serverWg.Wait() // Wait for the main server goroutines to finish
	//s.grTracking.grWg.Wait() // Ensure goroutines are fully completed

	if s.listener != nil {
		s.listener.Close()
	}
	if s.nodeListener != nil {
		s.nodeListener.Close()
	}

	close(s.quitCtx) // Close quit channel
	log.Println("Server shutdown complete")
}

//=======================================================

//=======================================================

func (s *GBServer) resolveConfigSeedAddr() error {

	// Check if no seed servers are configured
	if len(s.gbConfig.SeedServers) == 0 {
		// Ensure s.nodeTCPAddr is initialized
		if s.nodeTCPAddr == nil {
			return fmt.Errorf("nodeTCPAddr is not initialized")
		}
		// Use this node's TCP address as the seed
		s.seedAddr = append(s.seedAddr, s.nodeTCPAddr)
		log.Printf("seed server list --> %v\n", s.seedAddr)
		return nil
	}

	if len(s.gbConfig.SeedServers) >= 1 {

		for i := 0; i < len(s.gbConfig.SeedServers); i++ {
			addr := net.JoinHostPort(s.gbConfig.SeedServers[i].SeedIP, s.gbConfig.SeedServers[i].SeedPort)
			tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
			if err != nil {
				return err
			}
			s.seedAddr = append(s.seedAddr, tcpAddr)
			log.Printf("seed server list --> %v\n", s.seedAddr)
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

	log.Printf("Starting client accept loop -- %s\n", name)

	log.Printf("Creating client listener on %s\n", s.clientTCPAddr.String())

	l, err := s.listenConfig.Listen(s.serverContext, s.clientTCPAddr.Network(), s.clientTCPAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	// Add listener to the server
	s.listener = l

	// Can begin go-routine for accepting connections
	go s.acceptConnection(l, "client-test", func(conn net.Conn) { s.createClient(conn, "normal-client", false, CLIENT) })

}

//TODO Figure out how to manage routines and shutdown signals

func (s *GBServer) AcceptNodeLoop(name string) {

	log.Printf("Starting node accept loop -- %s\n", name)

	log.Printf("Creating node listener on %s\n", s.nodeTCPAddr.String())

	nl, err := s.listenConfig.Listen(s.serverContext, s.nodeTCPAddr.Network(), s.nodeTCPAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	s.nodeListener = nl

	//go s.acceptConnection(nl, "node-test", func(conn net.Conn) { s.createNodeClient(conn, "node-client", false, NODE) })
	s.startGoRoutine(s.ServerName, "accept node connection routine", func() {
		s.acceptConnection(nl, "node-test", func(conn net.Conn) { s.createNodeClient(conn, "node-client", false, NODE) })
	})

	//---------------- Seed Dial ----------------//
	//This is essentially a solicit
	if !s.isOriginal {
		// If we're not the original (seed) node, connect to the seed server
		//go s.connectToSeed()
		s.startGoRoutine(s.ServerName, "connect to seed routine", func() {
			defer s.grWg.Done()
			s.connectToSeed()
		})
	}

}

//=======================================================
// Accept Connection - taking connections from different listeners
//=======================================================

// TODO add a callback error function for any read errors to signal client closures?
// TODO consider adding client connection scoped context...?

func (s *GBServer) acceptConnection(l net.Listener, name string, createConnFunc func(conn net.Conn)) {

	defer s.grWg.Done()

	// TODO Need better signalling here to exit the accept loop
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-s.serverContext.Done():
				log.Println("Server context done")
				// Delete clients and close go-routines
				return
			default:
				log.Printf("Error accepting connection: %s\n", err)
				break
			}
		}

		s.startGoRoutine(s.ServerName, "create connection routine", func() {
			s.cRM.Lock()
			createConnFunc(conn)
			s.cRM.Unlock()
			s.grWg.Done()
		})
	}

}

//=======================================================
// Creating a node server
//=======================================================
