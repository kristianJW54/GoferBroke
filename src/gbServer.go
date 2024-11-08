package src

import (
	"context"
	"fmt"
	"io"
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
	Seed net.IP
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
	tcpAddr       *net.TCPAddr

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

	//Connection Handling
	phoneBook map[string]*net.Conn //Can we point to a wrapped conn struct which is designed for our use case?
	connMutex sync.RWMutex
	pool      sync.Pool // Maybe to use with varying buffer sizes

	cRM sync.RWMutex

	quitCtx chan struct{}
	done    chan bool
	ready   chan struct{}

	serverWg sync.WaitGroup
}

func NewServer(serverName string, config *Config, host string, port string, lc net.ListenConfig) *GBServer {

	addr := net.JoinHostPort(host, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
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
		tcpAddr:             tcpAddr,
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

	//Checks and other start up here

	//Need to:
	//	Initialise the cluster map and check for seed ip
	//	If seed ip is equal to our identity then we are the seed
	//
	//	Construct our identity
	//
	//	Figure out if we need to contact seed and perform update request to begin gossip
	//
	//	Establish time synchronisations

	// This needs to be a method with locks
	seed := s.config.Seed
	switch {
	case seed == nil:
		s.isOriginal = true
		s.itsWhoYouKnow.seedServer.seedAddr = s.tcpAddr
	case seed.Equal(s.tcpAddr.IP):
		s.isOriginal = true
	default:
		s.isOriginal = false
	}

	s.AcceptLoop("client-test")

}

// handle connections are within AcceptLoop which are their own go-routine and will have signals and context
//

func (s *GBServer) AcceptLoop(name string) {

	log.Printf("Starting accept loop -- %s\n", name)

	log.Printf("Creating listener on %s\n", s.BroadcastName)
	log.Printf("Seed Server %v\n", s.itsWhoYouKnow.seedServer.seedAddr.String())

	l, err := s.listenConfig.Listen(s.serverContext, s.tcpAddr.Network(), s.tcpAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	// Add listener to the server
	s.listener = l

	// Can begin go-routine for accepting connections
	go s.accept(l, "client-test")

}

// Clients are created and stored by the server to propagate during gossip with the mesh

//=======================================================

func (s *GBServer) accept(l net.Listener, name string) {

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
			defer conn.Close()
			s.handle(conn)
		}()
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

func (s *GBServer) handle(conn net.Conn) {
	buf := make([]byte, MAX_MESSAGE_SIZE)

	//TODO Look at implementing a specific read function to handle our TCP Packets and have
	// our own protocol specific rules around read

	for {
		n, err := conn.Read(buf)
		if err != nil && err != io.EOF {
			log.Println("read error", err)
			return
		}
		if n == 0 {
			return
		}

		//TODO Implement a handler router for server-server connections and client-server connections
		// Similar to Nats where the read and write loop are run inside the handle (or in NATS case the connFunc)

		// Create a GossipPayload to unmarshal the received data into
		var dataPayload TCPPayload
		err = dataPayload.UnmarshallBinaryV1(buf[:n]) // Read the exact number of bytes
		if err != nil {
			log.Println("unmarshall error", err)
			return
		}

		// Log the decoded data (as a string)
		log.Printf("%v %v %v %v %s", dataPayload.Header.ProtoVersion, dataPayload.Header.MessageType, dataPayload.Header.Command, dataPayload.Header.MessageLength, string(dataPayload.Data))
	}

}

//=======================================================
