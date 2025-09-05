package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	uuid2 "github.com/google/uuid"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"github.com/kristianJW54/GoferBroke/internal/Network"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"io"
	"log"
	"log/slog"
	"math"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//Server runs the core program and logic for a node and is the entry point to the system. Every node is a server.

const ServerNameMaxLength = 32 - (8 + 1)

func Run(ctx context.Context, w io.Writer, mode, name string, routes []string, clusterNetwork, nodeAddr, clientPort, nodeFileConfig, clusterFileConfig, profiling string) error {

	log.SetOutput(w)

	nodeIP, nodePort := strings.Split(nodeAddr, ":")[0], strings.Split(nodeAddr, ":")[1]

	if nodePort == clientPort {
		return fmt.Errorf("node port must be different from the client port - [NODE] %s - [CLIENT] %s", nodePort, clientPort)
	}

	if nodePort == profiling {
		return fmt.Errorf("cannot start profiling on same port as node")
	}

	// Create a new context that listens for interrupt signals
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	lc := net.ListenConfig{}

	var config *GbClusterConfig
	var nodeConfig *GbNodeConfig

	nodeConfig = InitDefaultNodeConfig()

	if clientPort != "" {
		nodeConfig.ClientPort = clientPort
	}

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

	// Create and start the server
	gbs, err := NewServer(name, config, nil, nodeConfig, nodeIP, nodePort, nodeConfig.ClientPort, lc)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	go func() {
		fmt.Println("Starting server...")
		if profiling != "" {
			startPprofServer("127.0.0.1", profiling)
		}
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
	ServerID
	// ServerName is the official name of the server which is UUID@TIMESATMP and is what other nodes in the cluster
	// identify the server with. ServerName is also used in our view of the cluster map as our own self info - we access
	// it and address it the same as any other node
	ServerName string
	//time of server creation - can point to ServerID timestamp
	initialised uint64
	// Host IP of the server at start
	host string
	// Port the server can be reached on
	port string
	addr string
	// boundTCPAddr is the join of host and port
	boundTCPAddr *net.TCPAddr
	// advertiseAddress is the resolved address and the address which we advertise to other nodes when we connect
	advertiseAddress *net.TCPAddr
	clientTCPAddr    *net.TCPAddr
	// reachability describes the type of network this node is subscribed to LOCAL/DYNAMIC/PUBLIC/PRIVATE
	reachability Network.NodeNetworkReachability

	//Distributed Info

	// gossip embeds the control structure and parameters for gossip engine
	gossip         *gossip
	convergenceEst time.Duration
	// isSeed declares if this server is a seed or not
	isSeed bool
	// canBeRendezvous is used for NAT Traversal and hole punching
	// TODO To implement
	canBeRendezvous bool
	// TODO To implement
	discoveryPhase bool

	//Events
	pendingHandlerRegs []HandlerRegistrationFunc
	event              *EventDispatcher
	fatalErrorCh       chan error

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

	// Failure
	fail *failureControl

	//Connection Handling
	gcid                 uint64 // Global client ID counter
	numNodeConnections   int64  //Atomically incremented
	numClientConnections int64  //Atomically incremented

	clientStore          sync.Map
	tmpConnStore         sync.Map
	nodeConnStore        sync.Map
	notToGossipNodeStore sync.Map

	// nodeReqPool is for the server when acting as a client/node initiating requests of other nodes
	//it must maintain a pool of active sequence numbers for open requests awaiting response
	nodeReqPool seqReqPool

	// Locks
	serverLock     sync.RWMutex
	clusterMapLock sync.RWMutex
	configLock     sync.RWMutex

	//serverWg *sync.WaitGroup
	startupSync *sync.WaitGroup

	//Logging
	logger      *slog.Logger
	slogHandler *fastLogger
	jsonBuffer  *jsonRingBuffer

	//Metrics
	sm    *systemMetrics
	smMap []string

	bj *backgroundJobs

	//go-routine tracking
	grTracking
}

type seedEntry struct {
	host     string
	port     string
	resolved *net.TCPAddr
	nodeID   string // This should be set when we make connection which will enable us to access the node conn in store and cluster map
}

// NewServerFromConfigFile take config file paths and parses into config structures and schema to build a new server. A
// new instance of GBServer is returned
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

// NewServerFromConfigString takes raw string inputs and parses into valid config structures and schemas. New GBServer instance
// is returned
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

// NewServerFromConfig takes either raw string or config structure inputs to build a new instance of GBServer from.
func NewServerFromConfig(nodeConfigData, clusterConfigData any) (*GBServer, error) {

	nodeCfg := InitDefaultNodeConfig()
	clusterCfg := InitDefaultClusterConfig()

	// Handle node config
	switch v := nodeConfigData.(type) {
	case string:
		_, err := BuildConfigFromString(v, nodeCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse node config: %w", err)
		}
	case GbNodeConfig:
		nodeCfg = &v
	case *GbNodeConfig:
		nodeCfg = v
	default:
		return nil, fmt.Errorf("invalid node config type %T, must be string or gbNodeConfig", v)
	}

	// Handle cluster config
	var sch map[string]*ConfigSchema
	switch v := clusterConfigData.(type) {
	case string:
		var err error
		sch, err = BuildConfigFromString(v, clusterCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cluster config: %w", err)
		}
	case GbClusterConfig:
		clusterCfg = &v
	case *GbClusterConfig:
		clusterCfg = v
	default:
		return nil, fmt.Errorf("invalid cluster config type %T, must be string or gbClusterConfig", v)
	}

	srv, err := NewServer(
		nodeCfg.Name,
		clusterCfg,
		sch,
		nodeCfg,
		nodeCfg.Host,
		nodeCfg.Port,
		nodeCfg.ClientPort,
		net.ListenConfig{},
	)
	if err != nil {
		return nil, err
	}

	return srv, nil
}

// NewServer is the main function to create a new instance of GBServer. It takes a node config and a cluster config as well as address values and client ports
// to construct a server that can be started.
func NewServer(serverName string, gbConfig *GbClusterConfig, schema map[string]*ConfigSchema, gbNodeConfig *GbNodeConfig, nodeHost string, nodePort, clientPort string, lc net.ListenConfig) (*GBServer, error) {

	if len([]byte(serverName)) > ServerNameMaxLength {
		return nil, fmt.Errorf("server name length exceeds %d", ServerNameMaxLength)
	}

	if nodePort == clientPort {
		return nil, fmt.Errorf("node port must be different from the client port - [NODE] %s - [CLIENT] %s", nodePort, clientPort)
	}

	logger, slogHandler, jsonBuffer := setupLogger(context.Background(), gbNodeConfig)

	addr := net.JoinHostPort(nodeHost, nodePort)
	nodeTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	clientAddr := net.JoinHostPort(nodeTCPAddr.IP.String(), clientPort)
	clientTCPAddr, err := net.ResolveTCPAddr("tcp", clientAddr)
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

	// Init gossip
	goss := initGossipSettings(1*time.Second, 1) //gbConfig.Cluster.NodeSelection

	seq := newSeqReqPool(10) //gbConfig.Cluster.RequestIDPool

	fail := newFailureControl(gbConfig)

	// Init metrics
	sm, smMap := newSystemMetrics()

	ctx, cancel := context.WithCancel(context.Background())

	bj := newBackgroundJobScheduler()

	s := &GBServer{
		ServerID:         *serverID,
		ServerName:       srvName,
		initialised:      uint64(serverID.timeUnix),
		host:             nodeHost,
		port:             nodePort,
		addr:             addr,
		boundTCPAddr:     nodeTCPAddr,
		advertiseAddress: nodeTCPAddr, //Temp set which will be updated once we dial a connection
		clientTCPAddr:    clientTCPAddr,
		reachability:     1, // TODO : Yet to implement

		pendingHandlerRegs: make([]HandlerRegistrationFunc, 0, 2),
		event:              NewEventDispatcher(),
		fatalErrorCh:       make(chan error, 1),

		listenConfig:        lc,
		ServerContext:       ctx,
		serverContextCancel: cancel,

		gbClusterConfig: gbConfig,
		gbNodeConfig:    gbNodeConfig,
		seedAddr:        seedAddr,
		configSchema:    cfgSchema,
		originalCfgHash: cfgHash,

		fail: fail,

		gossip:               goss,
		isSeed:               gbNodeConfig.IsSeed,
		canBeRendezvous:      false,
		numNodeConnections:   0,
		numClientConnections: 0,

		nodeReqPool: *seq,

		startupSync: &sync.WaitGroup{},

		logger:      logger,
		slogHandler: slogHandler,
		jsonBuffer:  jsonBuffer,

		sm:    sm,
		smMap: smMap,

		bj: bj,

		grTracking: grTracking{
			index:       0,
			numRoutines: 0,
			grWg:        &sync.WaitGroup{},
		},
	}

	isSeedAddr := s.seedCheck()

	switch {
	case !s.isSeed && isSeedAddr:
		return nil, fmt.Errorf("server is NOT configured as a seed, but matches a listed seed address — change config to mark it as a seed OR use a different address")

	case s.isSeed && !isSeedAddr:
		return nil, fmt.Errorf("server IS configured as a seed, but does not match any listed seed addresses — check seed list and address config")
	}

	if !gbNodeConfig.Internal.DisableStartupMessage {
		printStartup(s)
	}

	return s, nil
}

func (s *GBServer) initSelf() {

	defer s.startupSync.Done()

	// Then we initialise our info into a participant struct
	selfInfo, err := s.initSelfParticipant()
	if err != nil {
		return
	}

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

	s.notToGossipNodeStore.Store(s.ServerName, struct{}{})

	// register our background jobs
	s.bj.registerBackgroundJob(5*time.Second, 1*time.Second, s.checkSuspectedNode)
	s.bj.registerBackgroundJob(8*time.Second, 1*time.Second, s.runMetricCheck)

}

// StartServer should be run in a go-routine. Upon start, the server will check it's state and launch both internal and gossip processes once accept connection routines
// have successfully launched
func (s *GBServer) StartServer() {

	now := time.Now()

	// Reset the context to handle reconnect scenarios
	s.serverLock.Lock()
	s.resetContext()
	s.flags.clear(SHUTTING_DOWN)
	s.serverLock.Unlock()

	if !s.gbNodeConfig.Internal.DisableUpdateServerTimeStampOnStartup {

		s.updateTime(uint64(now.Unix())) // To sync to when the server is started
		s.initialised = uint64(now.Unix())
		srvName := s.String()
		s.ServerName = srvName
	}

	//s.clusterMapLock.Lock()
	if s.gbNodeConfig.Internal.DisableInitialiseSelf {
		s.logger.Warn("initialise self disabled")
	} else {
		// Debug logs here
		s.startupSync.Add(1)
		s.initSelf()
		// TODO next release - look at discovery phase
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

	s.startupSync.Add(1)
	err = s.AcceptNodeLoop()
	if err != nil {
		log.Fatal(err)
	}

	//---------------- Client Accept Loop ----------------//

	s.startupSync.Add(1)
	err = s.AcceptLoop()
	if err != nil {
		log.Fatal(err)
	}

	//---------------- Background Process Registers ----------------//

	//-- Start a background process to delete dead nodes
	//-- Start handler to collect gossip metrics
	s.startGoRoutine(s.PrettyName(), "background-process", func() {
		s.startBJ()
	})

	// We wait for start up to complete here
	s.startupSync.Wait()

	// Gossip process launches a sync.Cond wait pattern which will be signalled when connections join and leave using a connection check.
	if !s.gbNodeConfig.Internal.DisableGossip {

		// Start up phi process which will wait for the gossip signal
		//s.startGoRoutine(s.PrettyName(), "phi-process", func() {
		//	s.phiProcess(s.ServerContext)
		//})

		// Handle dead connection process here:
		//-->

		s.startGoRoutine(s.PrettyName(), "gossip-process",
			func() {
				defer s.gossipCleanup()
				s.gossipProcess(s.ServerContext)
			})
	}

	s.logger.Info("server started",
		slog.String("server", s.String()),
		slog.Time("started", now),
		slog.String("address", s.addr),
		slog.Bool("seed", s.isSeed),
		slog.String("client listener", s.clientTCPAddr.String()),
	)

}

//=======================================================
// Server Shutdown

// Shutdown attempts to gracefully shut down the server and terminate any running processes and go-routines. It will close listeners and client connections.
func (s *GBServer) Shutdown() {

	now := time.Now()

	defer func() {
		if s.slogHandler != nil {
			s.slogHandler.Close()
		}
	}()

	// Try to acquire the server lock
	if !s.serverLock.TryLock() { // Assuming TryLock is implemented
		return
	}
	defer s.serverLock.Unlock()

	// Log shutdown initiation

	// Cancel the server context to signal all other processes
	s.serverContextCancel()

	// Set the SHUTTING_DOWN flag to prevent new processes from starting
	s.flags.set(SHUTTING_DOWN)

	//log.Println("context called")

	if s.listener != nil {
		//log.Println("closing client listener")
		err := s.listener.Close()
		if err != nil {
			s.logger.Error("failed to close client listener")
		}
		s.listener = nil
	}
	if s.nodeListener != nil {
		//log.Println("closing node listener")
		err := s.nodeListener.Close()
		if err != nil {
			s.logger.Error("failed to close node listener")
		}
		s.nodeListener = nil
	}

	//Close connections
	s.tmpConnStore.Range(func(key, value interface{}) bool {
		c, ok := value.(*gbClient)
		if !ok {
			s.logger.Error("wrong client type when closing connections in tmpConnStore",
				"got", reflect.TypeOf(value))
			return true // Continue iteration
		}
		if c.gbc != nil {
			err := c.gbc.Close()
			if err != nil {
				s.logger.Error("failed to close client conn when closing connections in tmpConnStore",
					slog.String("name", c.name),
				)
			}
		}
		s.tmpConnStore.Delete(key)
		return true
	})

	s.clearNodeConnCount()

	s.nodeConnStore.Range(func(key, value interface{}) bool {
		c, ok := value.(*gbClient)
		if !ok {
			s.logger.Error("wrong client type when closing connections in nodeConnStore",
				"got", reflect.TypeOf(value))
			return true
		}
		if c.gbc != nil {
			err := c.gbc.Close()
			if err != nil {
				s.logger.Error("failed to close client conn when closing connections in nodeConnStore",
					slog.String("name", c.name),
				)
			}
		}
		s.nodeConnStore.Delete(key)
		return true
	})

	s.clientStore.Range(func(key, value interface{}) bool {
		c, ok := value.(*gbClient)
		if !ok {
			s.logger.Error("wrong client type when closing connections in clientStore",
				"got", reflect.TypeOf(value))
			return true
		}
		if c.gbc != nil {
			err := c.gbc.Close()
			if err != nil {
				s.logger.Error("failed to close client conn when closing connections in clientStore",
					slog.String("name", c.name),
				)
			}
		}
		s.nodeConnStore.Delete(key)
		return true
	})

	//s.serverLock.Unlock()
	s.grWg.Wait()

	s.logger.Info("shutdown complete",
		slog.Time("time", now),
		slog.Duration("duration", time.Since(now)),
		slog.Int("active go-routines", int(atomic.LoadInt64(&s.numRoutines))),
	)

}

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
		seedAddr = append(seedAddr, &seedEntry{
			host:     value.Host,
			port:     value.Port,
			resolved: tcpAddr,
		})
	}
	return seedAddr, nil
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
		return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, nil, "no seeds to dial")
	}

	// Special case: single seed
	if len(seeds) == 1 {
		seed := seeds[0]

		if seed.resolved.IP.Equal(s.boundTCPAddr.IP) && seed.resolved.Port == s.boundTCPAddr.Port {
			return nil, nil
		}

		// Check if already connected
		if conn, ok := s.nodeConnStore.Load(seed.nodeID); ok {
			return conn.(net.Conn), nil
		}

		// Try to connect
		conn, err := net.Dial("tcp", seed.resolved.String())
		if err != nil {
			return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, nil, "failed to dial single seed - %s", seed.nodeID)
		}

		if err := s.ComputeAdvertiseAddr(conn); err != nil {
			_ = conn.Close()
			return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, err, "")
		}

		return conn, nil
	}

	// Multiple seed retry logic
	retries := 3 // TODO: from config
	for i := 0; i < retries; i++ {
		addr, err := s.getRandomSeedToDial()
		if err != nil {
			return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, err, "")
		}

		if conn, ok := s.nodeConnStore.Load(addr.nodeID); ok {
			return conn.(net.Conn), nil
		}

		conn, err := net.Dial("tcp", addr.resolved.String())
		if err != nil {
			continue
		}

		if err := s.ComputeAdvertiseAddr(conn); err != nil {
			_ = conn.Close()
			return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, err, "")
		}

		return conn, nil
	}

	return nil, Errors.ChainGBErrorf(Errors.DialSeedErr, nil, "failed to dial single seed [%s]", seeds[0].host)
}

// seedCheck does a basic check to see if this server's address matches a configured seed server address.
func (s *GBServer) seedCheck() bool {
	if len(s.seedAddr) >= 1 {
		for _, addr := range s.seedAddr {
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

// initSelfParticipant builds a Participant on server startup which is used as the servers own info in the ClusterMap
// Here, deltas are initialised for standard internal keys such as ADDRESS and MEMORY etc.
// Config fields are also initialised as deltas to keep cluster config consistent across the cluster
func (s *GBServer) initSelfParticipant() (*Participant, error) {

	t := time.Now().Unix()

	p := &Participant{
		name:       s.ServerName,
		keyValues:  make(map[string]*Delta),
		maxVersion: t,
	}

	// Add the _ADDRESS_ delta
	addrDelta := &Delta{
		KeyGroup:  ADDR_DKG,
		Key:       _ADDRESS_,
		ValueType: D_INT64_TYPE,
		Version:   t,
		Value:     []byte(s.addr),
	}

	p.keyValues[MakeDeltaKey(addrDelta.KeyGroup, addrDelta.Key)] = addrDelta

	failureDelta := &Delta{
		KeyGroup:  FAILURE_DKG,
		Key:       s.ServerName,
		Version:   t,
		ValueType: D_UINT8_TYPE,
		Value:     []byte{ALIVE},
	}

	p.keyValues[MakeDeltaKey(failureDelta.KeyGroup, failureDelta.Key)] = failureDelta

	nameDelta := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _NODE_NAME_,
		ValueType: D_STRING_TYPE,
		Version:   t,
		Value:     []byte(s.name),
	}

	p.keyValues[MakeDeltaKey(nameDelta.KeyGroup, nameDelta.Key)] = nameDelta

	// TODO Implement in next release
	// Add the _REACHABLE_ delta
	//reachDelta := &Delta{
	//	KeyGroup:  NETWORK_DKG,
	//	Key:       _REACHABLE_,
	//	ValueType: D_INT_TYPE,
	//	Version:   t,
	//	Value:     []byte{byte(int(s.reachability))},
	//}

	//p.keyValues[MakeDeltaKey(reachDelta.KeyGroup, reachDelta.Key)] = reachDelta

	// Add the _NODE_CONNS_ delta
	numConns := uint16(0)
	numNodeConnBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(numNodeConnBytes, numConns)
	nodeConnsDelta := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _NODE_CONNS_,
		ValueType: D_UINT16_TYPE,
		Version:   t,
		Value:     numNodeConnBytes,
	}
	p.keyValues[MakeDeltaKey(nodeConnsDelta.KeyGroup, nodeConnsDelta.Key)] = nodeConnsDelta

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

	// System metrics
	//TODO Because it's the only instance i'm using external dependencies - I should build deltas in metrics.go and use
	// interfaces in case we want to change or build our own implementation

	// Memory deltas
	v, err := mem.VirtualMemory()
	if err != nil {
		s.logger.Error("failed to get memory info")
	} else {

		// Total memory
		total := make([]byte, 8)
		binary.BigEndian.PutUint64(total, v.Total)
		tm := &Delta{
			KeyGroup:  SYSTEM_DKG,
			Key:       _TOTAL_MEMORY_,
			ValueType: D_UINT64_TYPE,
			Version:   t,
			Value:     total,
		}
		p.keyValues[MakeDeltaKey(tm.KeyGroup, tm.Key)] = tm

		// Used memory
		used := make([]byte, 8)
		binary.BigEndian.PutUint64(used, v.Used)
		um := &Delta{
			KeyGroup:  SYSTEM_DKG,
			Key:       _USED_MEMORY_,
			ValueType: D_UINT64_TYPE,
			Version:   t,
			Value:     used,
		}
		p.keyValues[MakeDeltaKey(um.KeyGroup, um.Key)] = um

		// Free memory
		free := make([]byte, 8)
		binary.BigEndian.PutUint64(free, v.Free)
		fm := &Delta{
			KeyGroup:  SYSTEM_DKG,
			Key:       _FREE_MEMORY_,
			ValueType: D_UINT64_TYPE,
			Version:   t,
			Value:     free,
		}
		p.keyValues[MakeDeltaKey(fm.KeyGroup, fm.Key)] = fm

		// Perc used
		perc := make([]byte, 8)
		binary.BigEndian.PutUint64(perc, math.Float64bits(v.UsedPercent))
		pu := &Delta{
			KeyGroup:  SYSTEM_DKG,
			Key:       _MEM_PERC_,
			ValueType: D_FLOAT64_TYPE,
			Version:   t,
			Value:     perc,
		}
		p.keyValues[MakeDeltaKey(pu.KeyGroup, pu.Key)] = pu
	}

	// Host deltas
	h, _ := host.Info()

	// Host name
	hn := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _HOST_,
		ValueType: D_STRING_TYPE,
		Version:   t,
		Value:     []byte(h.Hostname),
	}
	p.keyValues[MakeDeltaKey(hn.KeyGroup, hn.Key)] = hn

	// Host ID
	hid := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _HOST_ID_,
		ValueType: D_STRING_TYPE,
		Version:   t,
		Value:     []byte(h.HostID),
	}
	p.keyValues[MakeDeltaKey(hid.KeyGroup, hid.Key)] = hid

	// CPU Deltas

	// CPU Model Name
	c, _ := cpu.Info()
	cmn := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _CPU_MODE_NAME_,
		ValueType: D_STRING_TYPE,
		Version:   t,
		Value:     []byte(c[0].ModelName),
	}
	p.keyValues[MakeDeltaKey(hid.KeyGroup, hid.Key)] = cmn

	// CPU Cores
	cores := make([]byte, 4)
	binary.BigEndian.PutUint32(cores, uint32(c[0].Cores))
	cc := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _CPU_CORES_,
		ValueType: D_INT32_TYPE,
		Version:   t,
		Value:     cores,
	}
	p.keyValues[MakeDeltaKey(cc.KeyGroup, cc.Key)] = cc

	// Platform Deltas

	pn := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _PLATFORM_,
		ValueType: D_STRING_TYPE,
		Version:   t,
		Value:     []byte(h.Platform),
	}
	p.keyValues[MakeDeltaKey(pn.KeyGroup, pn.Key)] = pn

	pf := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       _PLATFORM_FAMILY_,
		ValueType: D_STRING_TYPE,
		Version:   t,
		Value:     []byte(h.PlatformFamily),
	}
	p.keyValues[MakeDeltaKey(pf.KeyGroup, pf.Key)] = pf

	err = GenerateConfigDeltas(s.configSchema, s.gbClusterConfig, p)
	if err != nil {
		return nil, err
	}

	return p, nil

}

//=======================================================
// Accept Loops
//=======================================================

// AcceptLoop sets up a context and listener and then calls an internal acceptConnections method
func (s *GBServer) AcceptLoop() error {

	s.serverLock.Lock()

	ctx, cancel := context.WithCancel(s.ServerContext)
	defer cancel()

	l, err := s.listenConfig.Listen(s.ServerContext, s.clientTCPAddr.Network(), s.clientTCPAddr.String())
	if err != nil {
		return err
	}

	// Add listener to the server
	s.listener = l

	// Can begin go-routine for accepting connections
	go s.acceptConnection(l, "client-loop",
		func(conn net.Conn) {
			s.createClient(conn, CLIENT)
		},
		func(err error) bool {
			select {
			case <-ctx.Done():
				return true
			default:
				return false
			}
		})

	s.startupSync.Done()
	s.serverLock.Unlock()

	s.logger.Info("client accept loop started",
		slog.Time("time", time.Now()),
		slog.String("listener address", s.clientTCPAddr.String()))

	return nil

}

// AcceptNodeLoop sets up a context and listener and then calls an internal acceptConnection method
func (s *GBServer) AcceptNodeLoop() error {

	s.serverLock.Lock()

	ctx, cancel := context.WithCancel(s.ServerContext)
	defer cancel()

	nl, err := s.listenConfig.Listen(s.ServerContext, s.boundTCPAddr.Network(), s.boundTCPAddr.String())
	if err != nil {
		return err
	}

	s.nodeListener = nl

	s.startGoRoutine(s.PrettyName(), "accept-connection routine", func() {
		s.acceptConnection(nl, "node-loop",
			func(conn net.Conn) {
				s.createNodeClient(conn, "node-client", NODE)
			},
			func(err error) bool {
				select {
				case <-ctx.Done():
					return true
				default:
					return false
				}
			})
	})

	s.logger.Info("node accept loop started",
		slog.Time("time", time.Now()),
		slog.String("listener address", s.boundTCPAddr.String()))

	//---------------- Seed Dial ----------------//
	// This is essentially a solicit. If we are not a seed server then we must be the one to initiate a connection with the seed in order to join the cluster
	// A connectToSeed routine will be launched and blocks on a response. Once a response is given by the seed, we can move to connected state.

	if !s.isSeed || len(s.gbClusterConfig.SeedServers) > 1 {
		// If we're not the original (seed) node, connect to the seed server
		s.startGoRoutine(s.PrettyName(), "connect to seed routine", func() {
			err := s.connectToSeed()
			if err != nil {

				s.logger.Error("error connecting to seed",
					"err", err)

				s.Shutdown() //?? Do we do this here

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
				tmpDelay++
				time.Sleep(1 * time.Second)
				continue
			} else {
				break
			}
			continue
		}
		// go createFunc(conn)
		s.startGoRoutine(s.PrettyName(), "create connection routine", func() {
			createConnFunc(conn)
		})
	}
}

//=======================================================
// Internal Event Handler Registers
//=======================================================

func (s *GBServer) registerAndStartInternalHandlers() error {

	defer s.startupSync.Done()

	for _, reg := range s.pendingHandlerRegs {
		if err := reg(s); err != nil {
			s.logger.Error("failed to register external handler", "err", err)
		}
	}

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

	//Update our delta also
	v, err := encodeValue(D_UINT16_TYPE, uint16(s.numNodeConnections))
	if err != nil {
		s.logger.Info("err", "err", err)
		return
	}
	err = s.updateSelfInfo(&Delta{KeyGroup: SYSTEM_DKG, Key: _NODE_CONNS_, Version: time.Now().Unix(), ValueType: D_UINT16_TYPE, Value: v})
	if err != nil {
		s.logger.Error("error updating our delta node count", "err", err)
	}

	// Check and update gossip condition
	s.checkGossipCondition()
}

// Lock held on entry
// decrementNodeCount works the same as incrementNodeCount but by decrementing the node count and calling a check.
func (s *GBServer) decrementNodeConnCount() {
	// Atomically decrement node connections
	atomic.AddInt64(&s.numNodeConnections, -1)

	// Update our delta also
	v, err := encodeValue(D_UINT16_TYPE, uint16(s.numNodeConnections))
	if err != nil {
		s.logger.Info("err", "err", err)
		return
	}
	err = s.updateSelfInfo(&Delta{KeyGroup: SYSTEM_DKG, Key: _NODE_CONNS_, Version: time.Now().Unix(), ValueType: D_UINT16_TYPE, Value: v})
	if err != nil {
		s.logger.Error("error updating our delta node count", "err", err)
	}
	// Check and update gossip condition
	s.checkGossipCondition()
}

func (s *GBServer) clearNodeConnCount() {
	atomic.AddInt64(&s.numNodeConnections, 0)
}

//----------------
// Node Store

func (s *GBServer) getNodeConnFromStore(node string) (*gbClient, bool, error) {

	c, exists := s.nodeConnStore.Load(node)

	// Need to check if exists first
	if !exists {
		return nil, false, nil
	} else {
		gbc, ok := c.(*gbClient)
		if !ok {
			return nil, false, Errors.ChainGBErrorf(Errors.NodeConnStoreErr, nil, "conn is of wrong type - got %T want *gbClient", c)
		}
		return gbc, true, nil
	}
}

func (s *GBServer) activeNodeIDs() []string {
	var ids []string
	s.nodeConnStore.Range(func(key, val any) bool {
		// skip any nil or wrong‐type entries
		cli, ok := val.(*gbClient)
		if !ok || cli == nil {
			return true
		}
		id, ok := key.(string)
		if !ok {
			return true
		}
		ids = append(ids, id)
		return true
	})
	return ids
}

//---------------
// Add ID to seedAddr list

// addIDToSeedAddrList allows us to add the nodes ID which is the name of the node in relation to what is gossiped in the cluster and what is
// represented in the ClusterMap. By adding the node ID to the seed addr list we are able to easily identify seed nodes in the cluster and retrieve and reason about their
// addresses
func (s *GBServer) addIDToSeedAddrList(id string, addr net.Addr) error {

	s.serverLock.Lock()
	defer s.serverLock.Unlock()

	seeds := s.seedAddr

	for _, seed := range seeds {

		resolvedAddr, err := net.ResolveTCPAddr("tcp", addr.String())
		if err != nil {
			return Errors.ResolveSeedAddrErr
		}

		if seed.resolved.IP.Equal(resolvedAddr.IP) && seed.resolved.Port == resolvedAddr.Port {
			seed.nodeID = id
			return nil
		}

	}
	return Errors.ChainGBErrorf(Errors.ResolveSeedAddrErr, nil, "found no seed addressed in the list - looking for %s - for node %s", addr.String(), id)
}

//==================================================
// Self Info Handling + Monitoring
//==================================================

// every increase in node count will need to be updated in self info and max version updated.
// updateHeartBeat updates on every successful gossip with a node - the heartbeat and value should be updated
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

// UpdateClusterConfigDeltaAndSelf In this method we will have received a new cluster config value - we will have already updated our view of the participant in the
// cluster map but because this is a cluster config we will also need to update our own cluster map delta and finally apply that
// change to the actual cluster config struct of our server
func (s *GBServer) UpdateClusterConfigDeltaAndSelf(key string, d *Delta) error {

	s.serverLock.RLock()
	sch := s.configSchema
	cfg := s.gbClusterConfig
	s.serverLock.RUnlock()

	// If we have updated our own delta successfully we now try to update our server struct
	s.configLock.Lock()
	defer s.configLock.Unlock()

	decodedValue, err := decodeDeltaValue(d)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SelfConfigUpdateErr, err, "")
	}

	err = SetByPath(sch, cfg, key, decodedValue)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SelfConfigUpdateErr, err, "failed on key [%s]", key)
	}

	return nil

}

func (s *GBServer) getConvergenceEst() time.Duration {

	s.configLock.RLock()
	selection := s.gbClusterConfig.Cluster.NodeSelectionPerGossipRound
	interval := time.Second
	s.configLock.RUnlock()

	active := len(s.activeNodeIDs())

	log2N := math.Log2(float64(active))
	rounds := 10 * log2N / float64(selection)

	buffer := 2 * time.Second
	estimate := time.Duration(rounds * float64(interval))
	return estimate + buffer

}

//======================================================
// Background processes and job scheduler
//======================================================

type job struct {
	task      func()
	interval  time.Duration
	timeout   time.Duration
	nextRun   time.Time
	isRunning atomic.Bool
}

type backgroundJobs struct {
	jobs []*job
}

// tryRun launches the job once if not already running.
func (j *job) tryRun(parent context.Context) {
	if !j.isRunning.CompareAndSwap(false, true) {
		return // still executing
	}
	go func() {
		defer func() {
			j.isRunning.Store(false)
		}()
		j.task()
	}()
}

func newJob(interval time.Duration, timeout time.Duration, task func()) *job {
	return &job{
		task:     task,
		interval: interval,
		timeout:  timeout,
	}
}

func (bj *backgroundJobs) registerBackgroundJob(interval time.Duration, timeout time.Duration, task func()) {
	bj.jobs = append(bj.jobs, newJob(interval, timeout, task))
}

func newBackgroundJobScheduler() *backgroundJobs {
	return &backgroundJobs{
		jobs: make([]*job, 0),
	}
}

// Run in go-routine
func (s *GBServer) startBJ() {

	for {
		now := time.Now()
		soonest := now.Add(24 * time.Hour) // Arbitrary time which we will replace with job intervals

		// Go through the jobs
		for _, job := range s.bj.jobs {

			if job.nextRun.IsZero() {
				job.nextRun = now.Add(job.interval)
			}

			// If Jobs next run is due then we run it and add the next runtime to now
			if !now.Before(job.nextRun) {
				job.tryRun(s.ServerContext)
				// move ahead by interval to keep fixed cadence
				job.nextRun = job.nextRun.Add(job.interval)
				// guard against very small or zero interval
				if job.interval <= 0 {
					job.nextRun = now.Add(time.Second)
				}
			}

			// If our Job is before the soonest, then our job is next
			if job.nextRun.Before(soonest) {
				soonest = job.nextRun
			}
		}
		wait := time.Until(soonest)
		if wait < 0 {
			wait = 0
		}

		select {
		case <-time.After(wait):
		case <-s.ServerContext.Done():
			return
		}
	}
}
