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

	//Distributed Info
	isOriginal  bool
	isGossiping chan bool

	//Connection Handling
	tmpClientStore map[string]*gbClient
	//phoneBook      map[string]*gbClient
	//connMutex      sync.RWMutex
	//pool           sync.Pool // Maybe to use with varying buffer sizes

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

		isOriginal: false,

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
	//s.AcceptLoop("client-test")

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
