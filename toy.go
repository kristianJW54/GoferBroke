package main

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
)

type GBServer struct {
	ServerName          string
	Address             string
	Port                string
	listener            net.Listener
	listenConfig        net.ListenConfig
	status              int
	serverContext       context.Context
	serverContextCancel context.CancelFunc

	quitCtx  chan struct{}
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
		quitCtx:             make(chan struct{}),
	}

	return s
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
	defer s.listener.Close()

	// Can begin go-routine for accepting connections
	go s.accept(l, "client-test")

	<-s.quitCtx
	s.serverContextCancel()
}

// TODO create listner
// TODO create accept loop
// TODO handle shutdown signals and connections
// TODO Need shutdown method

// Clients are created and stored by the server to propagate during gossip with the mesh

func (s *GBServer) accept(l net.Listener, name string) {

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-s.serverContext.Done():
				log.Printf("Closing listener %s\n", name)
				return
			default:
				log.Printf("Error accepting connection: %s\n", err) // retry mechanism...?
				continue
			}
		}
		s.serverWg.Add(1)
		go func(c net.Conn) {
			defer s.serverWg.Done()
			defer c.Close()
			s.handle(c)
		}(conn)
	}

	//////////////

}

func (s *GBServer) Shutdown() {
	close(s.quitCtx)
	s.listener.Close()
	s.serverWg.Wait()
}

func (gbs *GBServer) handle(conn net.Conn) {
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

func (gbs *GBServer) Status() string {

	var stat string

	switch gbs.status {
	case 2:
		stat = "_CLOSED_"
	case 1:
		stat = "_ALIVE_"
	case 0:
		stat = "_OFF_"
	}
	return stat
}
