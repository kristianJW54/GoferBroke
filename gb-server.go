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
// - 5) Add comprehensive wrappers for go routine control and tracking

// Maybe want to declare some global const names for contexts -- seedServerContext -- nodeContext etc

type Config struct{} //Temp will be moved

type GBServer struct {
	//Server Info
	ServerName string //Set by config or flags
	addr       string
	tcpAddr    *net.TCPAddr
	// May want some resolver here to get name or something returnable for server info from the addr

	//TCP
	listener     net.Listener
	listenConfig net.ListenConfig

	//Context
	serverContext       context.Context
	serverContextCancel context.CancelFunc

	config *Config

	//Distributed Info
	theIn         string //Address and port of seed server -- config || flags
	itsWhoYouKnow map[string]string
	// func (ip IP) Equal(x IP) bool

	quitCtx chan struct{}
	done    chan bool
	ready   chan struct{}

	serverWg sync.WaitGroup
}

func NewServer(serverName string, network, host string, port string, lc net.ListenConfig) *GBServer {

	// TODO May want a more robust IP checking and resolving -- ?
	addr := net.JoinHostPort(host, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &GBServer{
		ServerName:          serverName,
		addr:                addr,
		tcpAddr:             tcpAddr,
		listenConfig:        lc,
		serverContext:       ctx,
		serverContextCancel: cancel,

		quitCtx: make(chan struct{}, 1),
		done:    make(chan bool, 1),
		ready:   make(chan struct{}, 1),
	}

	return s
}

func (s *GBServer) StartServer() {

	//Checks and other start up here

	s.AcceptLoop("client-test")

}

// Serve - Accept should be a go-routine which sits within a wait group or a blocking channel
// handle connections are within which are their own go-routine and will have signals and context

func (s *GBServer) AcceptLoop(name string) {

	log.Printf("Starting accept loop -- %s\n", name)

	log.Printf("Creating listener on %s\n", s.ServerName)

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

//=======================================================
