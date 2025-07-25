package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	uuid2 "github.com/google/uuid"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"github.com/kristianJW54/GoferBroke/internal/Network"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//Server runs the core program and logic for a node and is the entry point to the system. Every node is a server.

const ServerNameMaxLength = 32 - (8 + 1)

func Run(ctx context.Context, w io.Writer, mode, name string, routes []string, clusterNetwork, nodeAddr, nodeFileConfig, clusterFileConfig string) error {

	log.SetOutput(w)

	nodeIP, nodePort := strings.Split(nodeAddr, ":")[0], strings.Split(nodeAddr, ":")[1]

	// Create a new context that listens for interrupt signals
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	lc := net.ListenConfig{}

	var config *GbClusterConfig
	var nodeConfig *GbNodeConfig

	nodeConfig = InitDefaultNodeConfig()

	config = InitDefaultClusterConfig()

	var cn ClusterNetworkType

	cn, err := ParseClusterConfigNetworkType(clusterNetwork)

	if mode == "seed" {
		nodeConfig.IsSeed = true
	} else {
		nodeConfig.IsSeed = false
	}

	if len(routes) == 0 {
		ip := nodeIP
		port := nodePort

		// Initialize config with the seed server address
		config.SeedServers = append(config.SeedServers, &Seeds{
			Host: ip,
			Port: port,
		})
		config.Cluster.ClusterNetworkType = cn

	} else {

		var seeds []*Seeds

		for _, route := range routes {
			log.Printf("route = %s", route)
			ipPort := strings.Split(route, ":")
			if len(ipPort) != 2 {
				return fmt.Errorf("invalid seed route: %s", route)
			}
			seeds = append(seeds, &Seeds{
				Host: ipPort[0],
				Port: ipPort[1],
			})
		}

		config.SeedServers = seeds
		config.Cluster.ClusterNetworkType = cn

		fmt.Println("Config initialized:", config.SeedServers[0])
	}

	log.Printf("reached here")

	// Create and start the server
	gbs, err := NewServer(name, config, nil, nodeConfig, nodeIP, nodePort, "5000", lc)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	log.Printf("also reached here")

	go func() {
		fmt.Println("Starting server...")
		gbs.StartServer()
	}()

	<-ctx.Done()

	gbs.Shutdown()

	return nil
}

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
	GOSSIP_EXITED
	PHI_STARTED
	PHI_EXITED
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
	uuid     string
	timeUnix uint64
}

func NewServerID(name string, uuid string) *ServerID {
	return &ServerID{
		name:     name,
		uuid:     uuid,
		timeUnix: uint64(time.Now().Unix()),
	}
}

func (sf *ServerID) String() string {
	return fmt.Sprintf("%s@%v", sf.uuid, sf.timeUnix)
}

func (sf *ServerID) getID() string {
	return fmt.Sprintf("%s", sf.uuid)
}

func (sf *ServerID) PrettyName() string {
	return sf.name
}

func (sf *ServerID) updateTime(time uint64) {
	sf.timeUnix = time
}

//===================================================
// Main Server
//===================================================

// GBServer is the main server struct
type GBServer struct {
	//Server Info - can add separate info struct later
	ServerID
	ServerName       string //ID and timestamp
	initialised      uint64 //time of server creation - can point to ServerID timestamp
	host             string
	port             string
	addr             string
	boundTCPAddr     *net.TCPAddr
	advertiseAddress *net.TCPAddr
	clientTCPAddr    *net.TCPAddr
	reachability     Network.NodeNetworkReachability

	//Distributed Info
	gossip          *gossip
	isSeed          bool
	canBeRendezvous bool // TODO Change to reachable state machine
	discoveryPhase  bool

	//Events
	event        *EventDispatcher
	fatalErrorCh chan error

	flags serverFlags

	listener           net.Listener
	listenConfig       net.ListenConfig
	nodeListener       net.Listener
	nodeListenerConfig net.ListenConfig

	//Context
	ServerContext       context.Context
	serverContextCancel context.CancelFunc

	//Configuration
	gbClusterConfig *GbClusterConfig
	gbNodeConfig    *GbNodeConfig
	seedAddr        []*seedEntry
	originalCfgHash string
	configSchema    map[string]*ConfigSchema

	//Server Info for gossip
	clusterMap ClusterMap
	phi        phiControl

	//Connection Handling
	gcid uint64 // Global client ID counter
	// May need one for client and one for node as we will treat them differently
	numNodeConnections   int64 //Atomically incremented
	numClientConnections int64 //Atomically incremented

	// TODO Use sync.Map instead for connection storing
	clientStore map[uint64]*gbClient

	tmpConnStore         sync.Map
	nodeConnStore        sync.Map
	notToGossipNodeStore map[string]interface{}

	// nodeReqPool is for the server when acting as a client/node initiating requests of other nodes
	//it must maintain a pool of active sequence numbers for open requests awaiting response
	nodeReqPool seqReqPool

	// Locks
	serverLock     sync.RWMutex
	clusterMapLock sync.RWMutex
	configLock     sync.RWMutex

	//serverWg *sync.WaitGroup
	startupSync *sync.WaitGroup

	//go-routine tracking
	grTracking

	debugTrack int
}

type seedEntry struct {
	host     string
	port     string
	resolved *net.TCPAddr
	nodeID   string // This should be set when we make connection which will enable us to access the node conn in store and cluster map
}

func NewServerFromConfigFile(nodeConfigPath, clusterConfigPath string) (*GBServer, error) {

	nodeCfg := InitDefaultNodeConfig()
	clusterCfg := InitDefaultClusterConfig()

	_, err := BuildConfigFromFile(nodeConfigPath, nodeCfg)
	if err != nil {
		return nil, err
	}

	sch, err := BuildConfigFromFile(clusterConfigPath, clusterCfg)
	if err != nil {
		return nil, err
	}

	srv, err := NewServer(nodeCfg.Name, clusterCfg, sch, nodeCfg, nodeCfg.Host, nodeCfg.Port, nodeCfg.ClientPort, net.ListenConfig{})
	if err != nil {
		return nil, err
	}

	return srv, nil
}

func NewServerFromConfigString(nodeConfigData, clusterConfigData string) (*GBServer, error) {

	nodeCfg := InitDefaultNodeConfig()
	clusterCfg := InitDefaultClusterConfig()

	_, err := BuildConfigFromString(nodeConfigData, nodeCfg)
	if err != nil {
		return nil, err
	}

	sch, err := BuildConfigFromString(clusterConfigData, clusterCfg)
	if err != nil {
		return nil, err
	}

	srv, err := NewServer(nodeCfg.Name, clusterCfg, sch, nodeCfg, nodeCfg.Host, nodeCfg.Port, nodeCfg.ClientPort, net.ListenConfig{})
	if err != nil {
		return nil, err
	}

	return srv, nil
}

func NewServer(serverName string, gbConfig *GbClusterConfig, schema map[string]*ConfigSchema, gbNodeConfig *GbNodeConfig, nodeHost string, nodePort, clientPort string, lc net.ListenConfig) (*GBServer, error) {

	if len([]byte(serverName)) > ServerNameMaxLength {
		return nil, fmt.Errorf("server name length exceeds %d", ServerNameMaxLength)
	}

	addr := net.JoinHostPort(nodeHost, nodePort)
	nodeTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	nodeType, err := Network.DetermineNodeNetworkType(int(gbNodeConfig.NetworkType), nodeTCPAddr.IP)
	if err != nil {
		return nil, err
	}

	err = ConfigInitialNetworkCheck(gbConfig, gbNodeConfig, nodeType)
	if err != nil {
		return nil, err
	}

	cAddr := net.JoinHostPort(nodeHost, clientPort)

	// TODO Need to determine client network type as well
	clientAddr, err := net.ResolveTCPAddr("tcp", cAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Generates a server name object with name, uuid and time unix
	uuid := uuid2.New()
	serverID := NewServerID(serverName, uuid.String())
	// Joins the object to a string name
	srvName := serverID.String()

	// Config setting
	cfgHash, err := configChecksum(gbConfig)
	if err != nil {
		return nil, err
	}

	cfgSchema := schema
	if cfgSchema == nil {
		cfgSchema = BuildConfigSchema(gbConfig)
	}

	// Add seed addresses to seedAddr
	seedAddr, err := resolveConfigSeedAddr(gbConfig)
	if err != nil {
		return nil, err
	}

	// Build EventDispatcher
	ed := NewEventDispatcher()

	// Init gossip
	goss := initGossipSettings(1*time.Second, 1) //gbConfig.Cluster.NodeSelection

	seq := newSeqReqPool(10) //gbConfig.Cluster.RequestIDPool

	ctx, cancel := context.WithCancel(context.Background())

	s := &GBServer{
		ServerID:         *serverID,
		ServerName:       srvName,
		initialised:      uint64(serverID.timeUnix),
		host:             nodeHost,
		port:             nodePort,
		addr:             addr,
		boundTCPAddr:     nodeTCPAddr,
		advertiseAddress: nodeTCPAddr, //Temp set which will be updated once we dial a connection
		clientTCPAddr:    clientAddr,
		reachability:     1, // TODO : Yet to implement

		event:        ed,
		fatalErrorCh: make(chan error, 1),

		listenConfig:        lc,
		ServerContext:       ctx,
		serverContextCancel: cancel,

		gbClusterConfig: gbConfig,
		gbNodeConfig:    gbNodeConfig,
		seedAddr:        seedAddr,
		configSchema:    cfgSchema,
		originalCfgHash: cfgHash,

		clientStore: make(map[uint64]*gbClient),

		notToGossipNodeStore: make(map[string]interface{}),

		gossip:          goss,
		isSeed:          gbNodeConfig.IsSeed,
		canBeRendezvous: false,
		//numNodeConnections:   0,
		//numClientConnections: 0,

		nodeReqPool: *seq,

		startupSync: &sync.WaitGroup{},

		grTracking: grTracking{
			index:       0,
			numRoutines: 0,
			grWg:        &sync.WaitGroup{},
		},

		debugTrack: 0,
	}

	isSeedAddr := s.seedCheck()

	switch {
	case !s.isSeed && isSeedAddr:
		return nil, fmt.Errorf("server is NOT configured as a seed, but matches a listed seed address — change config to mark it as a seed OR use a different address")

	case s.isSeed && !isSeedAddr:
		return nil, fmt.Errorf("server IS configured as a seed, but does not match any listed seed addresses — check seed list and address config")
	}

	return s, nil
}

func (s *GBServer) initSelf() {

	defer s.startupSync.Done()

	// First init phi control
	s.phi = *s.initPhiControl()

	// Then we initialise our info into a participant struct
	selfInfo, err := s.initSelfParticipant()
	if err != nil {
		log.Printf("error in init participant %v", err)
		return
	}

	// Then we set up the mechanism to calculate phi score (controlled by phi control)
	selfInfo.paDetection = s.initPhiAccrual()

	// Now we initialise our cluster map and add our own info to it
	s.clusterMap = *initClusterMap(s.ServerName, s.boundTCPAddr, selfInfo)

	// Once we have that, if we are a seed - we should include our ID into the seedAddr list, so we don't contact or access ourselves
	if s.isSeed {
		for _, seed := range s.seedAddr {
			if seed.host == s.host && seed.port == s.port {
				seed.nodeID = s.String()
			}
		}
	}

	s.notToGossipNodeStore[s.ServerName] = &struct{}{}

}

// StartServer should be run in a go-routine. Upon start, the server will check it's state and launch both internal and gossip processes once accept connection routines
// have successfully launched
func (s *GBServer) StartServer() {

	// Reset the context to handle reconnect scenarios
	s.serverLock.Lock()
	s.resetContext()
	s.flags.clear(SHUTTING_DOWN)
	s.serverLock.Unlock()

	if !s.gbNodeConfig.Internal.DisableUpdateServerTimeStampOnStartup {

		now := uint64(time.Now().Unix())

		s.updateTime(now) // To sync to when the server is started
		s.initialised = now
		srvName := s.String()
		s.ServerName = srvName
	}

	if s.gbNodeConfig.Internal.IsTestMode {
		// Add debug mode output
		log.Printf("Server starting in test mode: %s\n", s.PrettyName())
	} else {
		fmt.Printf("Server starting: %s -- Part of %s\n", s.PrettyName(), s.gbClusterConfig.Name)

	}
	//s.clusterMapLock.Lock()
	if s.gbNodeConfig.Internal.DisableInitialiseSelf {
		log.Printf("[WARN] Config, Cluster Map, Phi Accrual, Self -> Not Initialised\n")
	} else {
		// Debug logs here
		s.startupSync.Add(1)
		s.initSelf()
		//if !s.isSeed {
		//	s.discoveryPhase = true
		//}
	}

	//-----------------------------------------------
	//Checks and other start up here

	// Setting go routine tracking flag to true - mainly used in testing
	s.grTracking.trackingFlag.Store(true)

	//---------------- Event Handler Registers ----------------//

	s.startupSync.Add(1)
	// We need to spin up event handlers here to catch any error during start up processes
	err := s.registerAndStartInternalHandlers()
	if err != nil {
		log.Fatal(err)
	}

	//---------------- Node Accept Loop ----------------//

	// Here we attempt to dial and connect to seed
	// TODO If we get a dial error being the same addr used on an active node then we signal shutdown immediately and send error event

	s.startupSync.Add(1)
	err = s.AcceptNodeLoop("node-test")
	if err != nil {
		log.Fatal(err)
	}

	//---------------- Client Accept Loop ----------------//
	//s.AcceptLoop("client-test")

	//TODO add monitoring routines to keep internal state up to date
	// CPU Metrics using an aggregate or significant change metric - how to signal?
	// can have a ticker monitoring which will signal a waiting loop for updating internal state

	//---------------- Internal Event Registers ----------------//

	//-- Start a background process to delete dead nodes
	//-- Start a background process to delete tombstone deltas

	// Main routines will be :....
	// - System monitoring
	// -- Memory used and stored by node, max delta size...
	// - Config changes
	// - State changes
	// - Use case assignments
	// - Handlers added
	// - Routing??

	// We wait for start up to complete here
	s.startupSync.Wait()

	// Gossip process launches a sync.Cond wait pattern which will be signalled when connections join and leave using a connection check.
	if !s.gbNodeConfig.Internal.DisableGossip {

		// Start up phi process which will wait for the gossip signal
		s.startGoRoutine(s.PrettyName(), "phi-process", func() {

			// Phi cleanup needed?
			s.phiProcess(s.ServerContext)

		})

		s.startGoRoutine(s.PrettyName(), "gossip-process",
			func() {
				defer s.gossipCleanup()
				s.gossipProcess(s.ServerContext)
			})
	}

	// May need to periodically check our node addr? and ensure we are resolved and reachable etc
}

//=======================================================
//---------------------
// Server Shutdown

//TODO - Thinking to add a tryShutdown which signals - logs time checks after a couple seconds and then checks
// flags and signals, if nothing then it simply calls it again

// Shutdown attempts to gracefully shut down the server and terminate any running processes and go-routines. It will close listeners and client connections.
func (s *GBServer) Shutdown() {

	// Try to acquire the server lock
	if !s.serverLock.TryLock() { // Assuming TryLock is implemented
		log.Printf("%s - Shutdown blocked waiting on serverLock", s.PrettyName())
		return
	}
	defer s.serverLock.Unlock()

	// Log shutdown initiation
	log.Printf("%s - Shutdown Initiated", s.PrettyName())

	// Cancel the server context to signal all other processes
	s.serverContextCancel()

	// Set the SHUTTING_DOWN flag to prevent new processes from starting
	s.flags.set(SHUTTING_DOWN)

	//log.Println("context called")

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
	s.tmpConnStore.Range(func(key, value interface{}) bool {
		c, ok := value.(*gbClient)
		if !ok {
			log.Printf("Error: expected *gbClient but got %T for key %v", value, key)
			return true // Continue iteration
		}
		if c.gbc != nil {
			log.Printf("%s -- closing temp node %s", s.PrettyName(), key)
			c.gbc.Close()
		}
		s.tmpConnStore.Delete(key)
		return true
	})

	s.clearNodeConnCount()

	s.nodeConnStore.Range(func(key, value interface{}) bool {
		c, ok := value.(*gbClient)
		if !ok {
			log.Printf("Error: expected *gbClient but got %T for key %v", value, key)
			return true
		}
		if c.gbc != nil {
			log.Printf("%s -- closing node %s", s.PrettyName(), key)
			c.gbc.Close()
		}
		s.nodeConnStore.Delete(key)
		return true
	})

	//s.serverLock.Unlock()

	//s.nodeReqPool.reqPool.Put(1)

	//log.Println("waiting...")
	s.grWg.Wait()
	//log.Println("done")

	if s.gbNodeConfig.Internal.IsTestMode {

		// Dump the deltas for checking
		for _, p := range s.clusterMap.participantArray {

			log.Printf("participant --> %s", p)

		}

	}

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
	s.ServerContext = ctx
	s.serverContextCancel = cancel
}

func resolveConfigSeedAddr(cfg *GbClusterConfig) ([]*seedEntry, error) {
	seedAddr := make([]*seedEntry, 0, len(cfg.SeedServers))

	for _, value := range cfg.SeedServers {
		addr := net.JoinHostPort(value.Host, value.Port)
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return nil, err
		}
		log.Printf("adding seed addr = %s", tcpAddr.String())
		seedAddr = append(seedAddr, &seedEntry{
			host:     value.Host,
			port:     value.Port,
			resolved: tcpAddr,
		})
	}
	return seedAddr, nil
}

func (s *GBServer) tryReconnectToSeed(connIndex int) (net.Conn, error) {

	s.serverLock.RLock()
	currAddr := s.seedAddr[connIndex]
	s.serverLock.RUnlock()

	addrResolve, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(currAddr.host, currAddr.port))
	if err != nil {
		return nil, err
	}

	if !currAddr.resolved.IP.Equal(addrResolve.IP) {
		s.serverLock.Lock()
		s.seedAddr[connIndex].resolved = addrResolve
		log.Printf("updating seed addr %v - from %s to %s", connIndex, currAddr.resolved.String(), addrResolve.String())
		s.serverLock.Unlock()
	}

	log.Printf("dialling again")
	conn, err := net.Dial("tcp", addrResolve.String())
	if err != nil {
		return nil, err
	}

	return conn, nil

}

func (s *GBServer) ComputeAdvertiseAddr(conn net.Conn) error {

	s.serverLock.RLock()
	advertise := s.advertiseAddress
	bound := s.boundTCPAddr
	s.serverLock.RUnlock()

	// If we have explicitly set and, it is NOT unspecified, use it
	if advertise != nil && !advertise.IP.IsUnspecified() {
		return nil
	}

	// If we bound to a specific IP and, it's not loopback, use that - if we are in a local cluster network it is fine also
	if bound != nil && !bound.IP.IsUnspecified() && (s.gbClusterConfig.Cluster.ClusterNetworkType == C_LOCAL || !bound.IP.IsLoopback()) {
		s.serverLock.Lock()
		s.advertiseAddress = &net.TCPAddr{IP: bound.IP, Port: bound.Port}
		s.addr = s.advertiseAddress.String()
		s.serverLock.Unlock()
		return nil
	}

	// If we have a connection to discover local from then try
	if conn != nil && bound != nil {
		local := conn.LocalAddr().(*net.TCPAddr)

		s.serverLock.Lock()
		s.advertiseAddress = &net.TCPAddr{
			IP:   local.IP,   // whatever IP the OS used
			Port: bound.Port, // your actual listening port
		}
		s.addr = s.advertiseAddress.String()
		s.serverLock.Unlock()
		return nil
	}

	// Fallback
	return Errors.UnableAdvertiseErr

}

func (s *GBServer) dialSeed() (net.Conn, error) {
	s.serverLock.RLock()
	seeds := s.seedAddr
	s.serverLock.RUnlock()

	if len(seeds) == 0 {
		return nil, fmt.Errorf("no seeds configured")
	}

	// Special case: single seed
	if len(seeds) == 1 {
		seed := seeds[0]

		if seed.resolved.IP.Equal(s.boundTCPAddr.IP) && seed.resolved.Port == s.boundTCPAddr.Port {
			log.Printf("I am the only seed — skipping dial")
			return nil, nil
		}

		// Check if already connected
		if conn, ok := s.nodeConnStore.Load(seed.nodeID); ok {
			log.Printf("Reusing existing connection to seed %s", seed.nodeID)
			return conn.(net.Conn), nil
		}

		// Try to connect
		conn, err := net.Dial("tcp", seed.resolved.String())
		if err != nil {
			return nil, fmt.Errorf("failed to dial single seed %s: %w", seed.nodeID, err)
		}

		if err := s.ComputeAdvertiseAddr(conn); err != nil {
			conn.Close()
			return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, err, "")
		}

		//s.nodeConnStore.Store(seed.nodeID, conn) // TODO Make sure we are storing elsewhere
		return conn, nil
	}

	// Multiple seed retry logic
	retries := 3 // TODO: from config
	for i := 0; i < retries; i++ {
		addr, err := s.getRandomSeedToDial()
		if err != nil {
			return nil, fmt.Errorf("failed to select a seed: %w", err)
		}

		if conn, ok := s.nodeConnStore.Load(addr.nodeID); ok {
			log.Printf("Reusing connection to seed %s", addr.resolved.String())
			return conn.(net.Conn), nil
		}

		conn, err := net.Dial("tcp", addr.resolved.String())
		if err != nil {
			log.Printf("Failed to dial seed %s: %v", addr.resolved.String(), err)
			continue
		}

		if err := s.ComputeAdvertiseAddr(conn); err != nil {
			conn.Close()
			return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, err, "")
		}

		//s.nodeConnStore.Store(addr.String(), conn) // TODO Make sure we are storing elsewhere
		return conn, nil
	}

	return nil, fmt.Errorf("failed to dial any seed after %d attempts", retries)
}

// seedCheck does a basic check to see if this server's address matches a configured seed server address.
func (s *GBServer) seedCheck() bool {

	if len(s.seedAddr) >= 1 {
		for _, addr := range s.seedAddr {
			//if addr.resolved.IP.Equal(s.advertiseAddress.IP) && addr.resolved.Port == s.advertiseAddress.Port {
			//	return true
			//}
			log.Printf("Checking seed %s - against us %s", addr.resolved.String(), s.boundTCPAddr.String())
			if addr.resolved.String() == s.boundTCPAddr.String() {
				return true
			}
		}
	}

	return false

}

//=======================================================
// Initialisation
//=======================================================

// TODO This needs to be a carefully considered initialisation which takes into account the server configurations

func initConnectionMetaData(reachableClaim int, givenAddr *net.TCPAddr, inbound bool) (*connectionMetaData, error) {

	var netType int

	if givenAddr != nil {
		reach, err := Network.DetermineNodeNetworkType(reachableClaim, givenAddr.IP)
		if err != nil {
			return nil, err
		}
		netType = int(reach)
	}

	return &connectionMetaData{
		inbound,
		givenAddr,
		reachableClaim,
		netType,
	}, nil

}

// TODO Need GBErrors Here--
// And environment + users use case + config map parsing of initialised delta map
func (s *GBServer) initSelfParticipant() (*Participant, error) {

	t := time.Now().Unix()

	p := &Participant{
		name:       s.ServerName,
		keyValues:  make(map[string]*Delta),
		maxVersion: t,
	}

	nameDelta := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _NODE_NAME_,
		ValueType: D_STRING_TYPE,
		Version:   t,
		Value:     []byte(s.name),
	}

	p.keyValues[MakeDeltaKey(nameDelta.KeyGroup, nameDelta.Key)] = nameDelta

	// Add the _ADDRESS_ delta
	addrDelta := &Delta{
		KeyGroup:  ADDR_DKG,
		Key:       _ADDRESS_,
		ValueType: D_INT64_TYPE,
		Version:   t,
		Value:     []byte(s.addr),
	}

	p.keyValues[MakeDeltaKey(addrDelta.KeyGroup, addrDelta.Key)] = addrDelta

	//TODO Think about how we implement reachability - what do we want to gossip and why
	// Are we letting the cluster know if our reachability can be used for NAT Traversal?

	// Add the _REACHABLE_ delta
	reachDelta := &Delta{
		KeyGroup:  NETWORK_DKG,
		Key:       _REACHABLE_,
		ValueType: D_INT_TYPE,
		Version:   t,
		Value:     []byte{byte(int(s.reachability))},
	}

	p.keyValues[MakeDeltaKey(reachDelta.KeyGroup, reachDelta.Key)] = reachDelta

	// Add the _NODE_CONNS_ delta
	numNodeConnBytes := make([]byte, 1)
	numNodeConnBytes[0] = 0
	nodeConnsDelta := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _NODE_CONNS_,
		ValueType: D_INT_TYPE,
		Version:   t,
		Value:     numNodeConnBytes,
	}
	p.keyValues[MakeDeltaKey(nodeConnsDelta.KeyGroup, nodeConnsDelta.Key)] = nodeConnsDelta

	// TODO Think about removing the heartbeat as we use PHI and don't actually use this
	// Add the _HEARTBEAT_ delta
	heart := make([]byte, 8)
	binary.BigEndian.PutUint64(heart, uint64(t))
	heartbeatDelta := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _HEARTBEAT_,
		ValueType: D_INT64_TYPE,
		Version:   t,
		Value:     heart,
	}
	p.keyValues[MakeDeltaKey(heartbeatDelta.KeyGroup, heartbeatDelta.Key)] = heartbeatDelta

	err := GenerateConfigDeltas(s.configSchema, s.gbClusterConfig, p)
	if err != nil {
		return nil, err
	}

	log.Printf("Initialised own deltas")

	return p, nil

}

//=======================================================
// Accept Loops
//=======================================================

// AcceptLoop sets up a context and listener and then calls an internal acceptConnections method
func (s *GBServer) AcceptLoop(name string) {

	s.serverLock.Lock()

	ctx, cancel := context.WithCancel(s.ServerContext)
	defer cancel() //TODO Need to think about context cancel for connection handling and retry logic/client disconnect

	//log.Printf("Starting client accept loop -- %s\n", name)

	//log.Printf("Creating client listener on %s\n", s.clientTCPAddr.String())

	l, err := s.listenConfig.Listen(s.ServerContext, s.clientTCPAddr.Network(), s.clientTCPAddr.String())
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
func (s *GBServer) AcceptNodeLoop(name string) error {

	s.serverLock.Lock()

	ctx, cancel := context.WithCancel(s.ServerContext)
	defer cancel() //TODO Need to think about context cancel for connection handling and retry logic/node disconnect

	//log.Printf("Starting node accept loop -- %s\n", name)

	//log.Printf("Creating node listener on %s\n", s.nodeTCPAddr.String())

	nl, err := s.listenConfig.Listen(s.ServerContext, s.boundTCPAddr.Network(), s.boundTCPAddr.String())
	if err != nil {
		return err // TODO Need GBError here
	}

	s.nodeListener = nl

	s.startGoRoutine(s.PrettyName(), "accept-connection routine", func() {
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

	//---------------- Seed Dial ----------------//
	// This is essentially a solicit. If we are not a seed server then we must be the one to initiate a connection with the seed in order to join the cluster
	// A connectToSeed routine will be launched and blocks on a response. Once a response is given by the seed, we can move to connected state.

	// If a seed node hasn't been added OR we have been started before the seed node then we can initiate a retry until fail and end server

	//TODO here we need to handle if connection returned is nil meaning seed is not live yet - do we want to retry the go-routine?
	// or retry within the connectToSeed method?
	// we would only want to retry if a batch of seed nodes were started together and needed a delay to wait for one or more to go live

	if !s.isSeed || len(s.gbClusterConfig.SeedServers) > 1 {
		// If we're not the original (seed) node, connect to the seed server
		//go s.connectToSeed()
		s.startGoRoutine(s.PrettyName(), "connect to seed routine", func() {
			err := s.connectToSeed()
			if err != nil {
				log.Printf("Error connecting to seed: %v", err)

				//// Dispatch event here - we don't need an event here as we can just call Shutdown() but the added context and ability to handle helps
				//s.DispatchEvent(Event{
				//	InternalError,
				//	time.Now().Unix(),
				//	&ErrorEvent{
				//		ConnectToSeed,
				//		Critical,
				//		Errors.ConnectSeedErr,
				//		"Accept Node Loop",
				//	},
				//	"Error in accept node loop when connecting to seed - shutting down - ensure seed server addresses are correct and live",
				//})
				//s.Shutdown() //??

				return
			}
		})
	}

	s.startupSync.Done()
	s.serverLock.Unlock()

	return nil

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
		s.startGoRoutine(s.PrettyName(), "create connection routine", func() {
			createConnFunc(conn)
		})
	}

	//log.Println("accept loop exited")

}

//=======================================================
// Internal Event Handler Registers
//=======================================================

func (s *GBServer) registerAndStartInternalHandlers() error {

	defer s.startupSync.Done()

	errCtx := &errorContext{
		s.ServerContext,
		&errorController{s: s},
		s.DispatchEvent,
		s.fatalErrorCh,
	}

	// Starting Internal Error handling event process
	if _, err := s.addInternalHandler(s.ServerContext, InternalError, func(event Event) error {
		err := handleInternalError(errCtx, event)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Next handler process here

	//--

	return nil

}

//=======================================================
// Sync Pool for Server-Server Request cycles
//=======================================================

// seqReqPool is a configurable number of request pools to create ID's for request-response cycles. When a node queues a message with an expected response, it will
// draw an ID from the pool and create a response handler to receive the response on a channel. The responding node will echo back the ID which will be matched to a
// handler and used to complete the request-response.
type seqReqPool struct {
	reqPool *sync.Pool
}

func newSeqReqPool(poolSize uint16) *seqReqPool {
	if poolSize > 65535 {
		poolSize = 65535
	} else if poolSize == 0 {
		poolSize = 100
	}

	sequence := make(chan uint16, poolSize)
	for i := 1; i < int(poolSize)+1; i++ {
		sequence <- uint16(i)
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

func (s *GBServer) acquireReqID() (uint16, error) {
	id := s.nodeReqPool.reqPool.Get()
	if id == nil {
		return 0, Errors.NoRequestIDErr
	}
	//log.Printf("acquiring id - %v", id)
	return id.(uint16), nil
}

func (s *GBServer) releaseReqID(id uint16) {
	//log.Printf("Releasing sequence ID %d back to the pool", id)
	s.nodeReqPool.reqPool.Put(id)
}

//----------------
// Connection Count

// incrementNodeCount atomically adds to the number of node connections. Once it does, it will call a check to take place to see if the change in conn count
// should signal the gossip process to either start or pause.
func (s *GBServer) incrementNodeConnCount() {
	// Atomically increment node connections
	atomic.AddInt64(&s.numNodeConnections, 1)
	//log.Printf("incrementing node conn count")
	// Check and update gossip condition
	s.checkGossipCondition()
}

// Lock held on entry
// decrementNodeCount works the same as incrementNodeCount but by decrementing the node count and calling a check.
func (s *GBServer) decrementNodeConnCount() {
	//log.Printf("removing conn count by 1")
	// Atomically decrement node connections
	atomic.AddInt64(&s.numNodeConnections, -1)
	// Check and update gossip condition
	s.checkGossipCondition()
}

func (s *GBServer) clearNodeConnCount() {

	atomic.AddInt64(&s.numNodeConnections, 0)

}

//----------------
// Node Store

// TODO Finish implementing - need to do a dial check so nodes can dial or if a valid error then return that instead
func (s *GBServer) getNodeConnFromStore(node string) (*gbClient, bool, error) {

	c, exists := s.nodeConnStore.Load(node)

	// Need to check if exists first
	if !exists {
		return nil, false, nil
	} else {
		gbc, ok := c.(*gbClient)
		if !ok {
			return nil, false, fmt.Errorf("%s - getNodeConnFromStore type assertion error for node %s - should be type gbClient got %T", s.ServerName, node, c)
		}
		return gbc, true, nil
	}
}

//---------------
// Add ID to seedAddr list

func (s *GBServer) addIDToSeedAddrList(id string, addr net.Addr) error {

	s.serverLock.Lock()
	defer s.serverLock.Unlock()

	seeds := s.seedAddr

	for _, seed := range seeds {

		resolvedAddr, err := net.ResolveTCPAddr("tcp", addr.String())
		if err != nil {
			return fmt.Errorf("failed to resolve address: %w", err)
		}

		if seed.resolved.IP.Equal(resolvedAddr.IP) && seed.resolved.Port == resolvedAddr.Port {
			seed.nodeID = id
			return nil
		}

	}

	return fmt.Errorf("found no seed addresses in the list - looking for %s - for node %s", addr.String(), id)

}

//==================================================
// Self Info Handling + Monitoring
//==================================================

// TODO think about where we need to place and handle updating our selfInfo map which is part of the cluster map we gossip about ourselves
// Example - every increase in node count will need to be updated in self info and max version updated
// Equally for heartbeats on every successful gossip with a node - the heartbeat and value should be updated

func (s *GBServer) updateHeartBeat(timeOfUpdate int64) error {
	self := s.GetSelfInfo()
	key := MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_)

	self.pm.Lock()
	defer self.pm.Unlock()

	if delta, exists := self.keyValues[key]; exists {
		binary.BigEndian.PutUint64(delta.Value, uint64(timeOfUpdate))
		delta.Version = timeOfUpdate
	} else {
		return fmt.Errorf("no heartbeat delta")
	}

	if timeOfUpdate > self.maxVersion {
		self.maxVersion = timeOfUpdate
	}

	return nil
}

// In this method we will have received a new cluster config value - we will have already updated our view of the participant in the
// cluster map but because this is a cluster config we will also need to update our own cluster map delta and finally apply that
// change to the actual cluster config struct of our server
// TODO Need to review where this should live
func (s *GBServer) updateClusterConfigDeltaAndSelf(key string, d *Delta) error {

	self := s.GetSelfInfo()
	s.serverLock.RLock()
	sch := s.configSchema
	cfg := s.gbClusterConfig
	s.serverLock.RUnlock()

	de := &DeltaUpdateEvent{
		DeltaKey: key,
	}

	err := self.Update(CONFIG_DKG, key, d, func(toBeUpdated, by *Delta) {
		if by.Version > toBeUpdated.Version {
			de.PreviousVersion = toBeUpdated.Version
			de.PreviousValue = bytes.Clone(toBeUpdated.Value)

			*toBeUpdated = *by

			de.CurrentVersion = toBeUpdated.Version
			de.CurrentValue = bytes.Clone(toBeUpdated.Value)
		}
	})

	if err != nil {
		handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {

			for _, gbErr := range gbError {

				log.Printf("gbErr = %s", gbErr.Error())

				if gbErr.Code == Errors.DELTA_UPDATE_NO_DELTA_CODE {
					log.Printf("config can't be updated with new fields")

					// TODO May want an internal error event here? To capture the config field that was new?

					return gbErr
				}

			}

			log.Printf("last error = %s", gbError[len(gbError)-1].Error())
			return nil

		})

		if handledErr != nil {
			log.Printf("error ----> %s", handledErr.Error())
			return Errors.ChainGBErrorf(Errors.SelfConfigUpdateErr, err, "failed on key [%s]", key)
		}

	}

	// If we have updated our own delta successfully we now try to update our server struct

	s.configLock.Lock()
	defer s.configLock.Unlock()

	decodedValue, err := decodeDeltaValue(d)
	if err != nil {
		return err
	}

	err = SetByPath(sch, cfg, key, decodedValue)
	if err != nil {
		log.Printf("error ----> %s", err.Error())

		return Errors.ChainGBErrorf(Errors.SelfConfigUpdateErr, err, "failed on key [%s]", key)
	}

	return nil

}
