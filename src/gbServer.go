package src

import (
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

//Server runs the core program and logic for a node and is the entry point to the system. Every node is a server.

//===================================================================================
// Server Flags
//===================================================================================

type serverFlags uint16

const (
	STARTED = 1 << iota
	SHUTTING_DOWN
	ACCEPT_LOOP_STARTED
	ACCEPT_NODE_LOOP_STARTED
	CONNECTED_TO_CLUSTER
	GOSSIP_SIGNALLED
	IS_GOSSIPING
)

//goland:noinspection GoMixedReceiverTypes
func (sf *serverFlags) set(s serverFlags) {
	*sf |= s
}

//goland:noinspection GoMixedReceiverTypes
func (sf *serverFlags) clear(s serverFlags) {
	*sf &= ^s
}

//goland:noinspection GoMixedReceiverTypes
func (sf serverFlags) isSet(s serverFlags) bool {
	return sf&s != 0
}

//goland:noinspection GoMixedReceiverTypes
func (sf *serverFlags) setIfNotSet(s serverFlags) bool {
	if *sf&s == 0 {
		*sf |= s
		return true
	}
	return false
}

//===================================================================================
// Main Server
//===================================================================================

// ServerID combines a server name, uuid and creation time into a broadcast name for other nodes to identify other nodes with
type ServerID struct {
	name     string
	uuid     int
	timeUnix uint64
}

func NewServerID(name string, uuid int) *ServerID {
	return &ServerID{
		name:     name,
		uuid:     uuid,
		timeUnix: uint64(time.Now().Unix()),
	}
}

func (sf *ServerID) String() string {
	return fmt.Sprintf("%s-%v@%v", sf.name, sf.uuid, sf.timeUnix)
}

func (sf *ServerID) getID() string {
	return fmt.Sprintf("%s-%v", sf.name, sf.uuid)
}

func (sf *ServerID) updateTime() {
	sf.timeUnix = uint64(time.Now().Unix())
}

// GBServer is the main server struct
type GBServer struct {
	//Server Info - can add separate info struct later
	ServerID
	ServerName    string //ID and timestamp
	initialised   int64  //time of server creation - can point to ServerID timestamp
	addr          string
	nodeTCPAddr   *net.TCPAddr
	clientTCPAddr *net.TCPAddr

	flags serverFlags

	listener           net.Listener
	listenConfig       net.ListenConfig
	nodeListener       net.Listener
	nodeListenerConfig net.ListenConfig

	//Context
	serverContext       context.Context
	serverContextCancel context.CancelFunc

	gbConfig *GbConfig
	seedAddr []*net.TCPAddr

	// Options - for config - tls etc...

	//Server Info for gossip
	selfInfo   *Participant
	clusterMap ClusterMap //Need pointer?

	// Configurations and extensibility should be handled in Options which will be embedded here
	//Distributed Info
	gossip     *gossip
	isOriginal bool
	// Metrics or values for gossip
	// CPU Load
	// Latency ...
	// Other Use Cases such as shard assignment, state, config changes etc, all should be gossiped

	//Connection Handling
	gcid uint64 // Global client ID counter
	// May need one for client and one for node as we will treat them differently
	numNodeConnections   int64 //Atomically incremented
	numClientConnections int64 //Atomically incremented

	tmpClientStore map[uint64]*gbClient
	nodeStore      map[string]*gbClient // TODO May want to possibly embed this into the clusterMap?
	clientStore    map[uint64]*gbClient

	// nodeReqPool is for the server when acting as a client/node initiating requests of other nodes
	//it must maintain a pool of active sequence numbers for open requests awaiting response
	nodeReqPool seqReqPool

	// Locks
	serverLock     sync.RWMutex
	clusterMapLock sync.RWMutex

	//serverWg *sync.WaitGroup
	startupSync *sync.WaitGroup

	//go-routine tracking
	grTracking
}

func NewServer(serverName string, uuid int, gbConfig *GbConfig, nodeHost string, nodePort, clientPort string, lc net.ListenConfig) *GBServer {

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

	serverID := NewServerID(serverName, uuid)
	srvName := serverID.String()

	// Creation steps
	// Gather server metrics

	// Init gossip
	goss := initGossipSettings(1*time.Second, 1)

	createdAt := time.Now()

	seq := newSeqReqPool(10)

	selfInfo := initSelfParticipant(srvName, addr)

	ctx, cancel := context.WithCancel(context.Background())

	s := &GBServer{
		ServerID:            *serverID,
		ServerName:          srvName,
		initialised:         createdAt.Unix(),
		addr:                addr,
		nodeTCPAddr:         nodeTCPAddr,
		clientTCPAddr:       clientAddr,
		listenConfig:        lc,
		serverContext:       ctx,
		serverContextCancel: cancel,

		gbConfig:       gbConfig,
		seedAddr:       make([]*net.TCPAddr, 0),
		tmpClientStore: make(map[uint64]*gbClient),
		nodeStore:      make(map[string]*gbClient),
		clientStore:    make(map[uint64]*gbClient),

		selfInfo:   selfInfo,
		clusterMap: *initClusterMap(srvName, nodeTCPAddr, selfInfo),

		gossip:     goss,
		isOriginal: false,
		//numNodeConnections:   0,
		//numClientConnections: 0,

		nodeReqPool: *seq,

		startupSync: &sync.WaitGroup{},

		// TODO Create init method and point to it here on server initialisation
		grTracking: grTracking{
			index:       0,
			numRoutines: 0,
			grWg:        &sync.WaitGroup{},
		},
	}

	return s
}

// StartServer should be run in a go-routine. Upon start, the server will check it's state and launch both internal and gossip processes once accept connection routines
// have successfully launched
func (s *GBServer) StartServer() {

	// Reset the context to handle reconnect scenarios
	s.resetContext()

	s.updateTime()
	srvName := s.String()
	s.ServerName = srvName

	//s.serverLock.Lock()

	fmt.Printf("Server starting: %s\n", s.ServerName)
	//fmt.Printf("Server address: %s, Seed address: %v\n", s.nodeTCPAddr, s.gbConfig.SeedServers)

	//Checks and other start up here
	//Resolve config seed addr
	err := s.resolveConfigSeedAddr()
	if err != nil {
		log.Fatal(err)
	}

	// Move this seed logic elsewhere
	if s.seedCheck() == 1 {
		s.isOriginal = true
	} else {
		s.isOriginal = false
	}

	// Setting go routine tracking flag to true - mainly used in testing
	s.grTracking.trackingFlag.Store(true)

	//s.serverLock.Unlock()

	//---------------- Node Accept Loop ----------------//
	s.startupSync.Add(1)
	s.AcceptNodeLoop("node-test")

	//---------------- Client Accept Loop ----------------//
	//s.AcceptLoop("client-test")

	//TODO add monitoring routines to keep internal state up to date
	// CPU Metrics using an aggregate or significant change metric - how to signal?
	// can have a ticker monitoring which will signal a waiting loop for updating internal state

	// Will need to start a monitoring internal state method which will spawn waiting go routines to monitor changes
	// internally and when signalled, update the changes by grabbing the server locks and unlocking after done

	// Also will need a start gossiping method call once this setup is done - this will start the gossiping routines
	// Which will use the cluster map etc...
	// Gossiping cannot start until the seeds have sent over the cluster map and state flag is moved from JOINING to CONNECTED_TO_CLUSTER

	// Main routines will be :....
	// - System monitoring
	// - Config changes
	// - State changes
	// - Use case assignments
	// - Handlers added
	// - Routing??

	// We wait for start up to complete here
	s.startupSync.Wait()

	// Gossip process launches a sync.Cond wait pattern which will be signalled when connections join and leave using a connection check.
	s.startGoRoutine(s.ServerName, "gossip-process",
		func() {
			s.gossipProcess()
		})

}

//=======================================================
//---------------------
// Server Shutdown

// Shutdown attempts to gracefully shutdown the server and terminate any running processes and go-routines. It will close listeners and client connections.
func (s *GBServer) Shutdown() {
	//log.Printf("%s -- shut down initiated\n", s.ServerName)
	s.serverLock.Lock()
	s.flags.set(SHUTTING_DOWN)

	//log.Println("context called")
	s.serverContextCancel()

	if s.listener != nil {
		//log.Println("closing client listener")
		s.listener.Close()
		s.listener = nil
	}
	if s.nodeListener != nil {
		//log.Println("closing node listener")
		s.nodeListener.Close()
		s.nodeListener = nil
	}

	//Close connections
	for name, client := range s.nodeStore {
		//log.Printf("%s closing client from NodeStore %s\n", s.ServerName, name)
		client.gbc.Close()
		delete(s.nodeStore, name)
	}

	for name, client := range s.tmpClientStore {
		//log.Printf("%s closing client from TmpStore %d\n", s.ServerName, name)
		client.gbc.Close()
		delete(s.tmpClientStore, name)
	}

	s.serverLock.Unlock()

	//s.nodeReqPool.reqPool.Put(1)

	//log.Println("waiting...")
	s.grWg.Wait()
	//log.Println("done")

	//log.Println("Server shutdown complete")
}

//=======================================================

//=======================================================

// resetContext if Server was Shutdown, then the context has been depleted and starting the server again will cause the old context
// to be used, therefore, a new context check must be done to provide a new one.
func (s *GBServer) resetContext() {
	// Cancel the old context if it exists
	if s.serverContextCancel != nil {
		s.serverContextCancel()
	}

	// Create a new context and cancel function
	ctx, cancel := context.WithCancel(context.Background())
	s.serverContext = ctx
	s.serverContextCancel = cancel
}

// TODO Resolver URL's also - to IPs and addr that can be stored as TCPAddr
// TODO Revisit when config parsing is complete
// TODO Think about different environments and addresses
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
		}
	}
	return nil
}

// seedCheck does a basic check to see if this server's address matches a configured seed server address.
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
// Initialisation
//=======================================================

// TODO This needs to be a carefully considered initialisation which takes into account the server configurations
// And environment + users use case
func initSelfParticipant(name, addr string) *Participant {

	t := time.Now().Unix()

	p := &Participant{
		name:       name,
		keyValues:  make(map[string]*Delta),
		deltaQ:     make(deltaHeap, 0), // Initialize an empty heap
		valueIndex: make([]string, 3),
		maxVersion: t,
	}

	// Add the _ADDRESS_ delta
	addrDelta := &Delta{
		key:       _ADDRESS_,
		valueType: INTERNAL_D,
		version:   t,
		value:     []byte(addr),
	}
	p.keyValues[_ADDRESS_] = addrDelta
	heap.Push(&p.deltaQ, addrDelta) // Add to heap

	// Add the _NODE_CONNS_ delta
	numNodeConnBytes := make([]byte, 1)
	numNodeConnBytes[0] = 0
	nodeConnsDelta := &Delta{
		key:       _NODE_CONNS_,
		valueType: INTERNAL_D,
		version:   t,
		value:     numNodeConnBytes,
	}
	p.keyValues[_NODE_CONNS_] = nodeConnsDelta
	heap.Push(&p.deltaQ, nodeConnsDelta) // Add to heap

	// Add the _HEARTBEAT_ delta
	heart := make([]byte, 8)
	binary.BigEndian.PutUint64(heart, uint64(t))
	heartbeatDelta := &Delta{
		key:       _HEARTBEAT_,
		valueType: INTERNAL_D,
		version:   t,
		value:     heart,
	}
	p.keyValues[_HEARTBEAT_] = heartbeatDelta
	heap.Push(&p.deltaQ, heartbeatDelta) // Add to heap

	return p

}

//=======================================================
// Accept Loops
//=======================================================

// AcceptLoop sets up a context and listener and then calls an internal acceptConnections method
func (s *GBServer) AcceptLoop(name string) {

	s.serverLock.Lock()

	ctx, cancel := context.WithCancel(s.serverContext)
	defer cancel() //TODO Need to think about context cancel for connection handling and retry logic/client disconnect

	//log.Printf("Starting client accept loop -- %s\n", name)

	//log.Printf("Creating client listener on %s\n", s.clientTCPAddr.String())

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

	s.serverLock.Unlock()
}

//TODO Figure out how to manage routines and shutdown signals

// AcceptNodeLoop sets up a context and listener and then calls an internal acceptConnection method
func (s *GBServer) AcceptNodeLoop(name string) {

	s.serverLock.Lock()

	ctx, cancel := context.WithCancel(s.serverContext)
	defer cancel() //TODO Need to think about context cancel for connection handling and retry logic/node disconnect

	//log.Printf("Starting node accept loop -- %s\n", name)

	//log.Printf("Creating node listener on %s\n", s.nodeTCPAddr.String())

	nl, err := s.listenConfig.Listen(s.serverContext, s.nodeTCPAddr.Network(), s.nodeTCPAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
	}

	s.nodeListener = nl

	s.startGoRoutine(s.ServerName, "accept-connection routine", func() {
		s.acceptConnection(nl, "node-test",
			func(conn net.Conn) {
				s.createNodeClient(conn, "node-client", false, NODE)
			},
			func(err error) bool {
				select {
				case <-ctx.Done():
					//log.Println("accept loop context canceled -- exiting loop")
					log.Println("Context canceled, exiting accept loop")
					return true
				default:
					//log.Printf("accept loop context error -- %s\n", err)
					return false
				}
			})
	})

	//time.Sleep(1 * time.Second)

	//---------------- Seed Dial ----------------//
	// This is essentially a solicit. If we are not a seed server then we must be the one to initiate a connection with the seed in order to join the cluster
	// A connectToSeed routine will be launched and blocks on a response. Once a response is given by the seed, we can move to connected state.
	if !s.isOriginal {
		// If we're not the original (seed) node, connect to the seed server
		//go s.connectToSeed()
		s.startGoRoutine(s.ServerName, "connect to seed routine", func() {
			s.connectToSeed()
			if err != nil {
				log.Printf("Error connecting to seed: %v", err)
				return
			}
		})
	}

	s.startupSync.Done()
	s.serverLock.Unlock()

}

//=======================================================
// Accept Connection - taking connections from different listeners
//=======================================================

// TODO add a callback error function for any read errors to signal client closures?
// TODO consider adding client connection scoped context...?

// acceptConnection takes a listener and uses two callback functions to create a client from the connection and kick out of the accept loop on error.
// As io.Reader read method blocks, we must exit on either read error or custom error from callback to successfully exit.
// createConnFunc creates a client we can manage and store as a connection.
func (s *GBServer) acceptConnection(l net.Listener, name string, createConnFunc func(conn net.Conn), customErr func(err error) bool) {

	s.flags.set(ACCEPT_NODE_LOOP_STARTED)

	delayCount := int(3)
	tmpDelay := int(0)

	for {
		conn, err := l.Accept()
		if err != nil {
			if customErr != nil && customErr(err) {
				//log.Println("custom error called")
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

	//log.Println("accept loop exited")

}

//=======================================================
// Sync Pool for Server-Server Request cycles
//=======================================================

// seqReqPool is a configurable number of request pools to create ID's for request-response cycles. When a node queues a message with an expected response, it will
// draw an ID from the pool and create a response handler to recieve the response on a channel. The responding node will echo back the ID which will be matched to a
// handler and used to complete the request-response.
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
					//log.Printf("Allocating sequence ID %d from the pool", id) // Log allocations
					return id
				default:
					//log.Println("Pool exhausted: no sequence IDs available")
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

//----------------
// Connection Count

// Lock should be held on entry
// incrementNodeCount atomically adds to the number of node connections. Once it does, it will call a check to take place to see if the change in conn count
// should signal the gossip process to either start or pause.
func (s *GBServer) incrementNodeConnCount() {
	// Atomically increment node connections
	atomic.AddInt64(&s.numNodeConnections, 1)
	// Check and update gossip condition
	s.checkGossipCondition()
}

// decrementNodeCount works the same as incrementNodeCount but by decrementing the node count and calling a check.
func (s *GBServer) decrementNodeConnCount() {
	log.Printf("removing conn count by 1")
	// Atomically decrement node connections
	atomic.AddInt64(&s.numNodeConnections, -1)
	// Check and update gossip condition
	s.checkGossipCondition()
}
