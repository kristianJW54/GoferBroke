package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	uuid2 "github.com/google/uuid"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"github.com/kristianJW54/GoferBroke/internal/Network"
	"io"
	"log"
	"maps"
	"net"
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

func Run(ctx context.Context, w io.Writer, mode, name string, routes []string, clusterNetwork, nodeAddr, nodeFileConfig, clusterFileConfig string) error {

	log.SetOutput(w)

	nodeIP, nodePort := strings.Split(nodeAddr, ":")[0], strings.Split(nodeAddr, ":")[1]

	// Create a new context that listens for interrupt signals
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	// TODO Need to complete this function - to seed checks and addr checks before creating servers
	// TODO Once server is created and started it is the job of dialling and exchanging to validate and fail etc -- need rules on this

	lc := net.ListenConfig{}

	var config *GbClusterConfig
	var nodeConfig *GbNodeConfig

	if nodeFileConfig == "" {
		// Load default config file from internal
		nodeConfig = &GbNodeConfig{
			Internal: &InternalOptions{},
		}
	}

	if clusterFileConfig == "" {
		config = &GbClusterConfig{}
	}

	var cn ClusterNetworkType

	cn, err := ParseClusterConfigNetworkType(clusterNetwork)

	// TODO This needs to change - cannot be localhost
	if len(routes) == 0 {
		ip := nodeIP // Use interface for now - will change when actual config is implemented
		port := nodePort

		// Initialize config with the seed server address
		config = &GbClusterConfig{
			SeedServers: []Seeds{
				{
					Host: ip,
					Port: port,
				},
			},
			Cluster: &ClusterOptions{
				ClusterNetworkType: cn,
			},
		}
		log.Println("Config initialized:", config)
	} else {

		log.Printf("cluster addr == %s", routes[0])

		clusterIP, clusterPort := strings.Split(routes[0], ":")[0], strings.Split(routes[0], ":")[1]

		config = &GbClusterConfig{
			SeedServers: []Seeds{
				{
					Host: clusterIP,
					Port: clusterPort,
				},
			},
			Cluster: &ClusterOptions{
				ClusterNetworkType: cn,
			},
		}
		log.Println("Config initialized:", config)
	}

	switch {
	case mode == "seed" || mode == "Seed":

		// First determine if we need to parser config files
		if clusterFileConfig != "" {
			log.Printf("need to parse config file")
			// clusterConfig will be created here
		} else {
			//config := &GbClusterConfig{}
		}

	}

	// Create and start the server
	gbs, err := NewServer(name, config, nodeConfig, nodeIP, nodePort, "5000", lc)
	if err != nil {
		return err
	}

	go func() {
		log.Println("Starting server...")
		gbs.StartServer()
	}()

	// Block until the context is canceled
	<-ctx.Done()

	log.Println("Shutting down server...")
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

func (sf *ServerID) updateTime() {
	sf.timeUnix = uint64(time.Now().Unix())
}

// GBServer is the main server struct
type GBServer struct {
	//Server Info - can add separate info struct later
	ServerID
	ServerName       string //ID and timestamp
	initialised      int64  //time of server creation - can point to ServerID timestamp
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
	event *EventDispatcher

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

	configFields  []string
	configSetters map[string]clusterConfigSetterMapFunc
	configGetters map[string]clusterConfigGetterMapFunc

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

	tmpConnStore  sync.Map
	nodeConnStore sync.Map

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
}

func NewServer(serverName string, gbConfig *GbClusterConfig, gbNodeConfig *GbNodeConfig, nodeHost string, nodePort, clientPort string, lc net.ListenConfig) (*GBServer, error) {

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
	srvName := uuid.String()

	// Creation steps

	// Build EventDispatcher
	ed := NewEventDispatcher()

	// Init gossip
	goss := initGossipSettings(1*time.Second, 1) //gbConfig.Cluster.NodeSelection

	seq := newSeqReqPool(10) //gbConfig.Cluster.RequestIDPool

	ctx, cancel := context.WithCancel(context.Background())

	s := &GBServer{
		ServerID:      *serverID,
		ServerName:    srvName,
		initialised:   int64(serverID.timeUnix),
		host:          nodeHost,
		port:          nodePort,
		addr:          addr,
		boundTCPAddr:  nodeTCPAddr,
		clientTCPAddr: clientAddr,
		reachability:  1, // TODO : Yet to implement

		event: ed,

		listenConfig:        lc,
		ServerContext:       ctx,
		serverContextCancel: cancel,

		gbClusterConfig: gbConfig,
		gbNodeConfig:    gbNodeConfig,
		seedAddr:        make([]*seedEntry, 0, 2),

		configFields: getConfigFields(reflect.ValueOf(gbConfig), ""),

		clientStore: make(map[uint64]*gbClient),

		gossip:          goss,
		isSeed:          false,
		canBeRendezvous: false,
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

		debugTrack: 0,
	}

	return s, nil
}

func (s *GBServer) initSelf() {

	defer s.startupSync.Done()

	s.phi = *s.initPhiControl()
	selfInfo, err := s.initSelfParticipant()
	if err != nil {
		return
	}
	selfInfo.paDetection = s.initPhiAccrual()
	s.clusterMap = *initClusterMap(s.ServerName, s.boundTCPAddr, selfInfo)
	s.configSetters = buildConfigSetters(s.gbClusterConfig, s.configFields)
	s.configGetters = buildConfigGetters(s.gbClusterConfig, s.configFields)

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
		s.updateTime() // To sync to when the server is started
		srvName := s.String()
		s.ServerName = srvName
	}

	if s.gbNodeConfig.Internal.IsTestMode {
		// Add debug mode output
		log.Printf("Server starting in test mode: %s\n", s.PrettyName())
	} else {
		fmt.Printf("Server starting: %s\n", s.PrettyName())

	}
	//s.clusterMapLock.Lock()
	if s.gbNodeConfig.Internal.DisableInitialiseSelf {
		log.Printf("[WARN] Config, Cluster Map, Phi Accrual, Self -> Not Initialised\n")
	} else {
		// Debug logs here
		s.startupSync.Add(1)
		s.initSelf()
	}

	//-----------------------------------------------
	//Checks and other start up here

	//Resolve config seed addr
	err := s.resolveConfigSeedAddr()
	if err != nil {
		log.Fatal(err)
	}

	if !s.isSeed {
		s.seedCheck()
	}

	// Setting go routine tracking flag to true - mainly used in testing
	s.grTracking.trackingFlag.Store(true)

	//---------------- Event Handler Registers ----------------//

	s.startupSync.Add(1)
	// We need to spin up event handlers here to catch any error during start up processes
	err = s.registerAndStartInternalHandlers()
	if err != nil {
		log.Fatal(err)
	}

	//---------------- Node Accept Loop ----------------//

	// Here we attempt to dial and connect to seed

	s.startupSync.Add(1)
	s.AcceptNodeLoop("node-test")

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

	// Start up phi process which will wait for the gossip signal
	s.startGoRoutine(s.PrettyName(), "phi-process", func() {

		// Phi cleanup needed?
		s.phiProcess(s.ServerContext)

	})

	// Gossip process launches a sync.Cond wait pattern which will be signalled when connections join and leave using a connection check.
	if !s.gbNodeConfig.Internal.DisableGossip {
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

func (s *GBServer) resolveConfigSeedAddr() error {

	if len(s.gbClusterConfig.SeedServers) >= 1 {

		s.serverLock.Lock()
		defer s.serverLock.Unlock()

		for _, value := range s.gbClusterConfig.SeedServers {
			addr := net.JoinHostPort(value.Host, value.Port)
			tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
			if err != nil {
				return err
			}
			s.seedAddr = append(s.seedAddr, &seedEntry{host: value.Host, port: value.Port, resolved: tcpAddr})
		}

	}
	return nil
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

	// If we bound to a specific IP and, it's not loopback, use that - if we in a local cluster network it is fine also
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

	for i, addr := range seeds {

		if addr.resolved.IP.Equal(s.boundTCPAddr.IP) && addr.resolved.Port == s.boundTCPAddr.Port {
			log.Printf("skipping same seed addr")
			continue
		}

		// TODO Need a connection check here so we don't redial and already made connection

		if s.isSeed {
			log.Printf("dialing other seed addr %s", addr.host)
		}

		conn, err := net.Dial("tcp", addr.resolved.String())
		if err != nil {
			log.Printf("Failed to dial seed %s: %v -- trying reconnect...", addr.host, err)
			conn, err = s.tryReconnectToSeed(i)
			if err != nil {
				continue
			}
		}

		// Connection succeeded, check if we need to update local advertise address
		err = s.ComputeAdvertiseAddr(conn)
		if err != nil {
			return conn, Errors.WrapGBError(Errors.DialSeedErr, err)
		}

		return conn, nil
	}

	return nil, nil
}

// seedCheck does a basic check to see if this server's address matches a configured seed server address.
func (s *GBServer) seedCheck() {

	if len(s.seedAddr) >= 1 {
		for _, addr := range s.seedAddr {
			if addr.resolved.IP.Equal(s.boundTCPAddr.IP) && addr.resolved.Port == s.boundTCPAddr.Port {
				s.isSeed = true
			}
		}
	}

	s.isSeed = false

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

func initConfigDeltas(configGetters map[string]clusterConfigGetterMapFunc, fields []string) (map[string]*Delta, error) {

	deltas := make(map[string]*Delta, len(fields))

	now := time.Now().Unix()

	for _, f := range fields {

		if value, ok := configGetters[f]; ok {

			typ, val, err := value(f)
			if err != nil {
				return nil, err
			}

			b, err := encodeGetterValue(val)
			if err != nil {
				return nil, err
			}

			d := &Delta{
				KeyGroup:  CONFIG_DKG,
				Key:       f,
				Version:   now,
				ValueType: typ,
				Value:     b,
			}

			deltas[MakeDeltaKey(CONFIG_DKG, f)] = d

		}

	}

	return deltas, nil

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

	cfgKV, err := initConfigDeltas(s.configGetters, s.configFields)
	if err != nil {
		return nil, err
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

	maps.Copy(p.keyValues, cfgKV)

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
func (s *GBServer) AcceptNodeLoop(name string) {

	s.serverLock.Lock()

	ctx, cancel := context.WithCancel(s.ServerContext)
	defer cancel() //TODO Need to think about context cancel for connection handling and retry logic/node disconnect

	//log.Printf("Starting node accept loop -- %s\n", name)

	//log.Printf("Creating node listener on %s\n", s.nodeTCPAddr.String())

	nl, err := s.listenConfig.Listen(s.ServerContext, s.boundTCPAddr.Network(), s.boundTCPAddr.String())
	if err != nil {
		log.Printf("Error creating listener: %s\n", err)
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

				// Dispatch event here - we don't need an event here as we can just call Shutdown() but the added context and ability to handle helps
				s.DispatchEvent(Event{
					InternalError,
					time.Now().Unix(),
					&ErrorEvent{
						ConnectToSeed,
						Critical,
						Errors.ConnectSeedErr,
						"Accept Node Loop",
					},
					"Error in accept node loop when connecting to seed - shutting down - ensure seed server addresses are correct and live",
				})

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

//==================================================
// Self Info Handling + Monitoring
//==================================================

// TODO think about where we need to place and handle updating our selfInfo map which is part of the cluster map we gossip about ourselves
// Example - every increase in node count will need to be updated in self info and max version updated
// Equally for heartbeats on every successful gossip with a node - the heartbeat and value should be updated

func updateHeartBeat(self *Participant, timeOfUpdate int64) error {

	key := MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_)

	if addr, exists := self.keyValues[key]; exists {

		binary.BigEndian.PutUint64(addr.Value, uint64(timeOfUpdate))

		self.pm.Lock()
		self.keyValues[key].Version = timeOfUpdate
		self.pm.Unlock()

	} else {
		return fmt.Errorf("no heartbeat delta")
	}

	return nil

}
