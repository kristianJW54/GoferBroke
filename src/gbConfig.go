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
	cluster     *clusterOptions
	internal    *internalOptions
}

type Seeds struct {
	SeedIP   string
	SeedPort string
}

type clusterOptions struct {
	maxGossipSize               uint16
	maxDeltaSize                uint16
	maxNumberOfNodes            int
	defaultSubsetDigestNodes    int
	maxSequenceIDPool           int
	nodeSelectionPerGossipRound int
	maxParticipantHeapSize      int
}

type internalOptions struct {
	//
	isTestMode                            bool
	disableInitialiseSelf                 bool
	disableGossip                         bool
	goRoutineTracking                     bool
	debugMode                             bool
	disableInternalGossipSystemUpdate     bool
	disableUpdateServerTimeStampOnStartup bool
	// Need logging config also
}

type testConfig struct {
}
