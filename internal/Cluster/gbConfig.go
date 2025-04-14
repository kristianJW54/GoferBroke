package Cluster

// TODO
// Need two top level different configs - cluster config shared by all nodes and gossiped changes
// Node config specific to the local node

//=============================================================
// Server Options + Config
//=============================================================

/*

Config will have two types [CLUSTER] + [INTERNAL]

Cluster will be gossiped and synced across the cluster for uniformity
- Cluster config will make other nodes also change their state

Internal will be local to the node - such as node name or address etc.
- Internal will only affect other nodes in so much as what is reflected in their map

*/

const (
	DEFAULT_MAX_DELTA_SIZE            = DEFAULT_MAX_GSA - 400 // TODO If config not set then we default to this
	DEFAULT_MAX_DISCOVERY_SIZE        = 1024
	DEFAULT_DISCOVERY_PERCENTAGE      = 50
	DEFAULT_MAX_GSA                   = 1400
	DEFAULT_MAX_DELTA_PER_PARTICIPANT = 5
	DEFAULT_PA_WINDOW_SIZE            = 100
	DEFAULT_PA_THRESHOLD              = 8
)

// TODO May want a config mutex lock?? -- Especially if gossip messages will mean our server makes changes to it's config

type GbClusterConfig struct {
	SeedServers map[string]Seeds `gb:"seed"`
	Cluster     *ClusterOptions
}

type Seeds struct {
	Name     string
	SeedHost string
	SeedPort string
}

type ClusterNetworkType int

const (
	C_UNDEFINED ClusterNetworkType = iota
	C_PRIVATE
	C_PUBLIC
	C_DYNAMIC
)

type ClusterOptions struct {
	maxGossipSize                 uint16
	maxDeltaSize                  uint16
	maxDiscoverySize              uint16
	discoveryPercentageProportion int
	maxNumberOfNodes              int
	defaultSubsetDigestNodes      int
	maxSequenceIDPool             int
	nodeSelectionPerGossipRound   int
	maxParticipantHeapSize        int
	preferredAddrGroup            string
	discoveryPercentage           int8 // from 0 to 100 how much of a percentage a new node should gather address information in discovery mode for based on total number of participants in the cluster
	paWindowSize                  int
	paThreshold                   int
	clusterNetworkType            ClusterNetworkType
}

type NodeNetworkType int

const (
	UNDEFINED NodeNetworkType = iota
	PRIVATE
	PUBLIC
)

type GbNodeConfig struct {
	Host        string
	Port        string
	NetworkType NodeNetworkType
	ClientPort  string
	Internal    *InternalOptions
}

type InternalOptions struct {
	//
	isTestMode                            bool
	disableInitialiseSelf                 bool
	disableGossip                         bool
	goRoutineTracking                     bool
	debugMode                             bool
	disableInternalGossipSystemUpdate     bool
	disableUpdateServerTimeStampOnStartup bool
	addressKeyGroup                       map[string][]string // Network group e.g. cloud, local, public and then list of ADDR keys e.g. IPv4, IPv6, WAN
	addressKeys                           []string

	// Need logging config also
}

//=====================================================================

//TODO Need config initializer here to set values and any defaults needed

// TODO need update functions and methods for when server runs background processes to update config based on gossip
