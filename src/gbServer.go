package src

import (
	"bufio"
	"context"
	"encoding/binary"
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

	fmt.Printf("Server starting: %s\n", s.ServerName)
	fmt.Printf("Server address: %s, Seed address: %s\n", s.tcpAddr, s.config.Seed)

	//Checks and other start up here

	// This needs to be a method with locks

	// Move this seed logic elsewhere
	seed := s.config
	switch {
	case seed.Seed == nil:
		// If the Seed is nil, we are the original (seed) node
		s.isOriginal = true
		s.itsWhoYouKnow.seedServer.seedAddr = s.tcpAddr
	case seed.Seed.IP.Equal(s.tcpAddr.IP) && seed.Seed.Port == s.tcpAddr.Port:
		// If the seed's IP and Port match our own TCP address, we're the original (seed) node
		s.isOriginal = true
	default:
		// Otherwise, we're not the original seed node
		s.isOriginal = false
	}

	//---------------- Accept Loop ----------------//
	s.AcceptLoop("client-test") //TODO Need to look at this

	fmt.Printf("%s %v\n", s.ServerName, s.isOriginal)

	//---------------- Seed Dial ----------------//
	if !s.isOriginal {
		// If we're not the original (seed) node, connect to the seed server
		s.connectToSeed()
	}

}

func (s *GBServer) connectToSeed() error {

	//With this function - we reach out to seed - so in our connection handling we would need to check protocol version
	//To understand how this connection is communicating ...

	//Create info message
	data := []byte(s.ServerName)

	header := newProtoHeader(1, 0, 4, 0)

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

	log.Printf("Starting accept loop -- %s\n", name)

	log.Printf("Creating listener on %s\n", s.BroadcastName)
	log.Printf("Seed Server %v %v\n", s.config.Seed, s.config.Seed.Port)

	l, err := s.listenConfig.Listen(s.serverContext, s.tcpAddr.Network(), s.tcpAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	// Add listener to the server
	s.listener = l

	// Can begin go-routine for accepting connections
	go s.accept(l, "client-test") // TODO Need to make inti a client management with routines for read + write for both server and client types

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
			s.readWireLoop(conn) // This is a new connection entry point - once in here we can handle client type connection loops etc
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

// Handle a pre []byte which is a payload and non-packet header

type ReadWireControl struct {
	buffer      []byte
	bytesRead   int
	shrinkCount int32
	offset      int
}

func (rc *ReadWireControl) parsePacket() {

	if rc.offset >= 4 {
		msgLength := int(binary.BigEndian.Uint32(rc.buffer[:4]))
		log.Printf("message length: %d\n", msgLength)

		if rc.offset >= 4+msgLength {
			message := rc.buffer[4 : 4+msgLength]
			log.Printf("complete message: %s\n", message)

			remainingData := rc.buffer[4+msgLength : rc.offset]
			copy(rc.buffer, remainingData)

			rc.offset -= 4 + msgLength
		}
	}
}

// This needs to be wrapped by client
func (s *GBServer) readWireLoop(conn net.Conn) {

	//initialise read controller here but it would have been done already once we embed within client
	rc := &ReadWireControl{
		buffer:      make([]byte, MIN_BUFF_SIZE),
		bytesRead:   0,
		shrinkCount: 0,
		offset:      0,
	}

	rdr := bufio.NewReader(conn)

	for {
		if len(rc.buffer)-rc.offset < MIN_BUFF_SIZE && len(rc.buffer) < MAX_BUFF_SIZE {
			newSize := len(rc.buffer) * 2
			if newSize > MAX_BUFF_SIZE {
				newSize = MAX_BUFF_SIZE
			}
			newBuf := make([]byte, newSize)
			copy(newBuf, rc.buffer[:rc.offset])
			rc.buffer = newBuf
			log.Printf("data %s", string(rc.buffer))
			log.Printf("increased buffer size: %d", newSize)
		}

		// Read data into the buffer starting at the offset
		n := rc.bytesRead
		n, err := rdr.Read(rc.buffer[:rc.offset])
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from connection: %s\n", err)
				return
			}
			log.Printf("error reading %v", err)
			return
		}

		log.Printf("data %s", string(rc.buffer[:n]))
		rc.offset += n
		rc.parsePacket()

		log.Printf("read %d bytes", n)
		log.Printf("current buffer usage: %d / %d", rc.bytesRead, len(rc.buffer))

	}

}

func (s *GBServer) handle(conn net.Conn) {
	buf := make([]byte, MAX_BUFF_SIZE)

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
		dataPayload := &TCPPacket{&PacketHeader{}, buf} //TODO This needs to a function to create a buffered payload

		err = dataPayload.UnmarshallBinaryV1(buf[:n]) // Read the exact number of bytes
		if err != nil {
			log.Println("unmarshall error", err)
			return
		}

		// Log the decoded data (as a string)
		log.Printf(
			"%v %v %v %v %v %s",
			dataPayload.Header.ProtoVersion,
			dataPayload.Header.ClientType,
			dataPayload.Header.MessageType,
			dataPayload.Header.Command,
			dataPayload.Header.PayloadLength,
			string(dataPayload.Data),
		)
	}

}

//=======================================================
