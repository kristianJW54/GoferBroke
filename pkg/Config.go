package pkg

import (
	"github.com/kristianJW54/GoferBroke/internal/cluster"
)

type GbClusterConfig struct {
	Name        string
	SeedServers []*Seeds
	Cluster     *ClusterOptions
}

type Seeds struct {
	Host string
	Port string
}

type ClusterNetworkType uint8

const (
	C_UNDEFINED ClusterNetworkType = iota
	C_PRIVATE
	C_PUBLIC
	C_DYNAMIC
	C_LOCAL
)

type ClusterOptions struct {
	NodeSelectionPerGossipRound    uint8
	DiscoveryPercentage            uint8 // from 0 to 100 how much of a percentage a new node should gather address information in discovery mode for based on total number of participants in the cluster
	MaxDeltaGossipedPerParticipant uint8
	MaxGossipSize                  uint16
	MaxDeltaSize                   uint16
	MaxDiscoverySize               uint16
	PaWindowSize                   uint16
	MaxGossSynAck                  uint16
	MaxNumberOfNodes               uint32
	GossipRoundTimeout             uint16 // ms
	MaxSequenceIDPool              uint32
	ClusterNetworkType             ClusterNetworkType
	// TODO Think if we need a URL map that users can specify for their own endpoints
	EndpointsURLMap map[string]string

	NodeMTLSRequired   bool `default:"false"`
	ClientMTLSRequired bool `default:"false"`

	// Failure
	FailureKNodesToProbe uint8
	FailureProbeTimeout  uint16 // ms

	//Background tasks
	NodeFaultyAfter uint16 // ms
}

func InitDefaultClusterConfig() *GbClusterConfig {

	ep := make(map[string]string)

	ep["test"] = "hello"

	return &GbClusterConfig{
		Name:        "",
		SeedServers: make([]*Seeds, 0, 4),
		Cluster: &ClusterOptions{
			NodeSelectionPerGossipRound:    cluster.DEFAULT_NODE_SELECTION_PER_ROUND,
			DiscoveryPercentage:            cluster.DEFAULT_DISCOVERY_PERCENTAGE,
			MaxDeltaGossipedPerParticipant: cluster.DEFAULT_MAX_DELTA_PER_PARTICIPANT,
			MaxGossipSize:                  cluster.DEFAULT_MAX_GOSSIP_SIZE,
			MaxDeltaSize:                   cluster.DEFAULT_MAX_DELTA_SIZE,
			MaxDiscoverySize:               cluster.DEFAULT_MAX_DISCOVERY_SIZE,
			PaWindowSize:                   cluster.DEFAULT_PA_WINDOW_SIZE,
			MaxGossSynAck:                  cluster.DEFAULT_MAX_GSA,
			MaxNumberOfNodes:               cluster.DEFAULT_MAX_NUMBER_OF_NODES,
			GossipRoundTimeout:             cluster.DEFAULT_GOSSIP_ROUND_TIMEOUT,
			MaxSequenceIDPool:              cluster.DEFAULT_SEQUENCE_ID_POOL,
			ClusterNetworkType:             C_UNDEFINED,
			EndpointsURLMap:                ep,
			NodeMTLSRequired:               false,
			ClientMTLSRequired:             false,
			FailureKNodesToProbe:           cluster.DEFAULT_FAILURE_PROBE,
			FailureProbeTimeout:            cluster.DEFAULT_FAILURE_TIMEOUT,
			NodeFaultyAfter:                cluster.DEFAULT_FAULTY_FLAG,
		},
	}

}

//=============================================================

type NodeNetworkType uint8

const (
	UNDEFINED NodeNetworkType = iota
	PRIVATE
	PUBLIC
	LOCAL
)

type GbNodeConfig struct {
	Name        string
	Host        string
	Port        string
	NetworkType NodeNetworkType
	IsSeed      bool
	ClientPort  string
	Internal    *InternalOptions
}

type InternalOptions struct {
	//
	IsTestMode                            bool
	DisableInitialiseSelf                 bool
	DisableGossip                         bool
	GoRoutineTracking                     bool
	DebugMode                             bool
	DisableInternalGossipSystemUpdate     bool
	DisableUpdateServerTimeStampOnStartup bool

	// Logging
	DefaultLoggerEnabled bool
	LogOutput            string
	LogToBuffer          bool
	LogBufferSize        int
	LogChannelSize       int

	// TLS
	CACertFilePath string
	CertFilePath   string
	KeyFilePath    string

	//Startup
	DisableStartupMessage bool

	// Need logging config also
}

func InitDefaultNodeConfig() *GbNodeConfig {
	return &GbNodeConfig{
		Name:        "node",
		Host:        "",
		Port:        "",
		NetworkType: UNDEFINED,
		IsSeed:      false,
		ClientPort:  "",
		Internal: &InternalOptions{
			IsTestMode:                            false,
			DisableInitialiseSelf:                 false,
			DisableGossip:                         false,
			GoRoutineTracking:                     true,
			DebugMode:                             false,
			DisableUpdateServerTimeStampOnStartup: false,
			DisableInternalGossipSystemUpdate:     false,

			DefaultLoggerEnabled: true,
			LogOutput:            "stdout",
			LogToBuffer:          true,
			LogBufferSize:        100,
			LogChannelSize:       200,

			CACertFilePath: "",
			CertFilePath:   "",
			KeyFilePath:    "",

			DisableStartupMessage: false,
		},
	}
}
