package main

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
)

//TODO -- STEPS FOR TCP ACCEPT LOOP SERVER CONTROL
// - 1) Will need a start function which creates a listener and initialises channels/processes
// 		- May want a StartListener function which takes a signal for when start up is complete
// 		- And then maybe a AcceptLoop from here
// - 2) Server Context and signals to control the server instance
// - 3) An Accept Loop function which sets up an accept loop and controls the internal accept go-routine
// - 4) An internal accept connection function which will hold the accept loop

type GBServer struct {
	ServerName          string
	Address             string
	Port                string
	listener            net.Listener
	listenConfig        net.ListenConfig
	status              int
	serverContext       context.Context
	serverContextCancel context.CancelFunc

	quitCtx chan struct{}
	done    chan bool
	ready   chan struct{}

	serverWg sync.WaitGroup
}

func NewServer(serverName string, address string, port string, lc net.ListenConfig) *GBServer {

	ctx, cancel := context.WithCancel(context.Background())

	s := &GBServer{
		ServerName:          serverName,
		Address:             address,
		Port:                port,
		listenConfig:        lc,
		status:              0,
		serverContext:       ctx,
		serverContextCancel: cancel,

		quitCtx: make(chan struct{}, 1),
		done:    make(chan bool, 1),
		ready:   make(chan struct{}, 1),
	}

	return s
}

func (s *GBServer) StartServer() {

	// Run checks
	// Reach out to clusters
	// Get info etc

	//s.serverWg.Add(1)
	//
	//go func() {
	//	defer s.serverWg.Done()
	//	for i := 0; i < 3; i++ {
	//		switch i {
	//		case 0:
	//			log.Println("Connecting...")
	//			time.Sleep(1 * time.Second)
	//		case 1:
	//			log.Println("Reaching out to cluster map")
	//			time.Sleep(1 * time.Second)
	//		case 2:
	//			log.Println("Gossiping with Seed Server")
	//			time.Sleep(1 * time.Second)
	//		}
	//	}
	//}()
	//s.serverWg.Wait()

	s.AcceptLoop("client-test")

}

// Serve - Accept should be a go-routine which sits within a wait group or a blocking channel
// handle connections are within which are their own go-routine and will have signals and context

func (s *GBServer) AcceptLoop(name string) {

	log.Printf("Starting accept loop -- %s\n", name)

	log.Printf("Creating listener on %s\n", s.ServerName)

	l, err := s.listenConfig.Listen(s.serverContext, "tcp", net.JoinHostPort(s.Address, s.Port))
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

func (s *GBServer) handle(conn net.Conn) {
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil && err != io.EOF {
			log.Println("read error", err)
			return
		}
		if n == 0 {
			return
		}
		log.Printf("received from %v: %s", conn.RemoteAddr(), string(buf[:n]))
	}
}

func (s *GBServer) Status() string {

	var stat string

	switch s.status {
	case 2:
		stat = "_CLOSED_"
	case 1:
		stat = "_ALIVE_"
	case 0:
		stat = "_OFF_"
	}
	return stat
}
