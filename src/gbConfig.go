package src

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
	DEFAULT_MAX_DELTA_SIZE            = 1024 // TODO If config not set then we default to this
	DEFAULT_MAX_GSA                   = DEFAULT_MAX_DELTA_SIZE * 2
	DEFAULT_MAX_DELTA_PER_PARTICIPANT = 5
)

// TODO May want a config mutex lock??

type GbConfig struct {
	SeedServers []Seeds `gb:"seed"`
	Cluster     *ClusterOptions
	Internal    *InternalOptions
}

type Seeds struct {
	SeedIP   string
	SeedPort string
}

type ClusterOptions struct {
	maxGossipSize               uint16
	maxDeltaSize                uint16
	maxNumberOfNodes            int
	defaultSubsetDigestNodes    int
	maxSequenceIDPool           int
	nodeSelectionPerGossipRound int
	maxParticipantHeapSize      int
	preferredAddrGroup          string
	discoveryPercentage         int8 // from 0 to 100 how much of a percentage a new node should gather address information in discovery mode for based on total number of participants in the cluster
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

	// Need logging config also
}

//=====================================================================
