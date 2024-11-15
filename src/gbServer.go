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
// - 1) Will need a start function which creates a listener and initialises channels/processes
// 		- May want a StartListener function which takes a signal for when start up is complete
// 		- And then maybe a AcceptLoop from here
// - 2) Server Context and signals to control the server instance
// - 3) An Accept Loop function which sets up an accept loop and controls the internal accept go-routine
// - 4) An internal accept connection function which will hold the accept loop
// - 5) Add comprehensive wrappers for go routine control and tracking

// CONSIDERATIONS //
// - TCP Keep-Alives default to no less than two hours which means that using anti-entropy with direct and in-direct
//		heartbeats along with gossip exchanges, the TCP connection will be kept alive unless detected otherwise by the
//		algorithm
//------------------------

// Maybe want to declare some global const names for contexts -- seedServerContext -- nodeContext etc

type Config struct {
	Seed *net.TCPAddr
} //Temp will be moved

type gbNet struct {
	net.Listener
	listenerConfig net.ListenConfig
}

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

	config *Config

	//Distributed Info
	isOriginal    bool
	itsWhoYouKnow *ClusterMap
	isGossiping   chan bool

	nodeListener       net.Listener
	nodeListenerConfig net.ListenConfig

	//Connection Handling
	clientCount int
	phoneBook   map[string]*gbClient
	connMutex   sync.RWMutex
	pool        sync.Pool // Maybe to use with varying buffer sizes

	cRM sync.RWMutex

	quitCtx chan struct{}
	done    chan bool
	ready   chan struct{}

	serverWg sync.WaitGroup
}

//TODO Add a node listener config + also client and node addr

func NewServer(serverName string, config *Config, nodeHost string, nodePort, clientPort string, lc net.ListenConfig) *GBServer {

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

		config: config,

		isOriginal:    false,
		itsWhoYouKnow: &ClusterMap{},

		quitCtx: make(chan struct{}, 1),
		done:    make(chan bool, 1),
		ready:   make(chan struct{}, 1),
	}

	return s
}

// Start server will be a go routine alongside this, the server will have to perform connection dials to other servers
// to maintain persistent connections and perform reconciliation of cluster map

func (s *GBServer) StartServer() {

	fmt.Printf("Server starting: %s\n", s.ServerName)
	fmt.Printf("Server address: %s, Seed address: %s\n", s.clientTCPAddr, s.config.Seed)

	//Checks and other start up here

	// This needs to be a method with locks

	// Move this seed logic elsewhere
	// TODO if we are not seed then we need to reach out - set a flag for this (initiator)
	seed := s.config
	switch {
	case seed.Seed == nil:
		// If the Seed is nil, we are the original (seed) node
		s.isOriginal = true
		s.itsWhoYouKnow.seedServer.seedAddr = s.nodeTCPAddr
	case seed.Seed.IP.Equal(s.nodeTCPAddr.IP) && seed.Seed.Port == s.nodeTCPAddr.Port:
		// If the seed's IP and Port match our own TCP address, we're the original (seed) node
		s.isOriginal = true
	default:
		// Otherwise, we're not the original seed node
		s.isOriginal = false
	}

	//---------------- Accept Loop ----------------//
	//TODO Need to make one for client and one for node
	s.AcceptLoop("client-test")

	fmt.Printf("%s %v\n", s.ServerName, s.isOriginal)

	//---------------- Seed Dial ----------------//
	if !s.isOriginal {
		// If we're not the original (seed) node, connect to the seed server
		s.connectToSeed()
	}

}

//=======================================================

func (s *GBServer) Shutdown() {
	s.serverContextCancel()
	if s.listener != nil {
		s.listener.Close()
	}

	close(s.quitCtx)

	log.Printf("Waiting for connections to close")
	s.serverWg.Wait()
}

//=======================================================

func (s *GBServer) connectToSeed() error {

	//With this function - we reach out to seed - so in our connection handling we would need to check protocol version
	//To understand how this connection is communicating ...

	//Create info message
	data := []byte(s.ServerName)

	header := newProtoHeader(1, 1)

	payload := &TCPPacket{
		header,
		data,
	}

	fmt.Println("Attempting to connect to seed server:", s.config.Seed.String())

	conn, err := net.Dial("tcp", s.config.Seed.String())
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}
	defer conn.Close()

	packet, err := payload.MarshallBinary()
	if err != nil {
		return fmt.Errorf("error marshalling payload: %s", err)
	}

	_, err = conn.Write(packet) // Sending the packet
	if err != nil {
		return fmt.Errorf("error writing to connection: %s", err)
	}

	return nil
}

// handle connections are within AcceptLoop which are their own go-routine and will have signals and context
//

func (s *GBServer) AcceptLoop(name string) {

	log.Printf("Starting client accept loop -- %s\n", name)

	log.Printf("Creating client listener on %s\n", s.BroadcastName)
	log.Printf("Seed Server %v\n", s.config.Seed)

	l, err := s.listenConfig.Listen(s.serverContext, s.nodeTCPAddr.Network(), s.clientTCPAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	// Add listener to the server
	s.listener = l

	// Can begin go-routine for accepting connections
	go s.acceptConnection(l, "client-test", func(conn net.Conn) { s.createNodeClient(conn, "node-client", NODE) })

}

func (s *GBServer) acceptConnection(l net.Listener, name string, createConnFunc func(conn net.Conn)) {

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-s.serverContext.Done():
				log.Println("Server context done")
				return
			default:
				log.Printf("Error accepting connection: %s\n", err)
				break
			}
		}
		s.serverWg.Add(1)
		go func() {
			defer s.serverWg.Done()
			createConnFunc(conn) // This is a new connection entry point - once in here we can handle client type connection loops etc
		}()
	}

}

//=======================================================
// Creating a node server
//=======================================================
