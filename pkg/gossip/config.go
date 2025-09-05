package gossip

import (
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"strings"
)

func BuildClusterConfig(clusterName string, setup func(config *ClusterConfig) error) (*ClusterConfig, error) {

	cfg := InitDefaultClusterConfig()
	cfg.Name = clusterName

	err := setup(cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil

}

type ClusterConfig struct {
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

func InitDefaultClusterConfig() *ClusterConfig {

	ep := make(map[string]string)

	ep["test"] = "hello"

	return &ClusterConfig{
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

func BuildNodeConfig(nodeName, nodeAddr string, setup func(cfg *NodeConfig) (*NodeConfig, error)) (*NodeConfig, error) {

	cfg := InitDefaultNodeConfig()
	cfg.Name = nodeName

	nodeIP, nodePort := strings.Split(nodeAddr, ":")[0], strings.Split(nodeAddr, ":")[1]
	cfg.Host = nodeIP
	cfg.Port = nodePort

	c, err := setup(cfg)
	if err != nil {
		return nil, err
	}

	return c, nil

}

type NodeConfig struct {
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

func InitDefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
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

func pkgNodeNetworkToNode(network NodeNetworkType) cluster.NodeNetworkType {
	switch network {
	case UNDEFINED:
		return cluster.UNDEFINED
	case PRIVATE:
		return cluster.PRIVATE
	case PUBLIC:
		return cluster.PUBLIC
	case LOCAL:
		return cluster.LOCAL
	default:
		return cluster.UNDEFINED
	}
}

func pkgNetworkTypeToCluster(networkType ClusterNetworkType) cluster.ClusterNetworkType {
	switch networkType {
	case C_LOCAL:
		return cluster.C_LOCAL
	case C_PRIVATE:
		return cluster.C_PRIVATE
	case C_PUBLIC:
		return cluster.C_PUBLIC
	case C_DYNAMIC:
		return cluster.C_DYNAMIC
	default:
		return cluster.C_UNDEFINED
	}
}

//======================================================
// Derive config from pkg

func DeriveFromPkg(pkgConfig any) (any, error) {

	switch cfg := pkgConfig.(type) {
	case *NodeConfig:

		nCfg := &cluster.GbNodeConfig{
			Name:        cfg.Name,
			Host:        cfg.Host,
			Port:        cfg.Port,
			IsSeed:      cfg.IsSeed,
			ClientPort:  cfg.ClientPort,
			NetworkType: pkgNodeNetworkToNode(cfg.NetworkType),
		}

		options := &cluster.InternalOptions{
			IsTestMode:                            cfg.Internal.IsTestMode,
			DisableInitialiseSelf:                 cfg.Internal.DisableInitialiseSelf,
			DisableGossip:                         cfg.Internal.DisableGossip,
			GoRoutineTracking:                     cfg.Internal.GoRoutineTracking,
			DebugMode:                             cfg.Internal.DebugMode,
			DisableInternalGossipSystemUpdate:     cfg.Internal.DisableInternalGossipSystemUpdate,
			DisableUpdateServerTimeStampOnStartup: cfg.Internal.DisableUpdateServerTimeStampOnStartup,
			DefaultLoggerEnabled:                  cfg.Internal.DefaultLoggerEnabled,
			LogOutput:                             cfg.Internal.LogOutput,
			LogToBuffer:                           cfg.Internal.LogToBuffer,
			LogBufferSize:                         cfg.Internal.LogBufferSize,
			LogChannelSize:                        cfg.Internal.LogChannelSize,
			CACertFilePath:                        cfg.Internal.CACertFilePath,
			CertFilePath:                          cfg.Internal.CertFilePath,
			KeyFilePath:                           cfg.Internal.KeyFilePath,
			DisableStartupMessage:                 cfg.Internal.DisableStartupMessage,
		}

		nCfg.Internal = options

		return nCfg, nil
	case *ClusterConfig:

		rCfg := &cluster.GbClusterConfig{
			Name: cfg.Name,
		}

		seeds := make([]*cluster.Seeds, 0, len(cfg.SeedServers))
		for _, s := range cfg.SeedServers {
			seeds = append(seeds, &cluster.Seeds{Host: s.Host, Port: s.Port})
		}
		rCfg.SeedServers = seeds

		options := &cluster.ClusterOptions{
			NodeSelectionPerGossipRound:    cfg.Cluster.NodeSelectionPerGossipRound,
			DiscoveryPercentage:            cfg.Cluster.DiscoveryPercentage,
			MaxDeltaGossipedPerParticipant: cfg.Cluster.MaxDeltaGossipedPerParticipant,
			MaxGossipSize:                  cfg.Cluster.MaxGossipSize,
			MaxDeltaSize:                   cfg.Cluster.MaxDeltaSize,
			MaxDiscoverySize:               cfg.Cluster.MaxDiscoverySize,
			PaWindowSize:                   cfg.Cluster.PaWindowSize,
			MaxGossSynAck:                  cfg.Cluster.MaxGossSynAck,
			MaxNumberOfNodes:               cfg.Cluster.MaxNumberOfNodes,
			GossipRoundTimeout:             cfg.Cluster.GossipRoundTimeout,
			MaxSequenceIDPool:              cfg.Cluster.MaxSequenceIDPool,
			ClusterNetworkType:             pkgNetworkTypeToCluster(cfg.Cluster.ClusterNetworkType),
			NodeMTLSRequired:               cfg.Cluster.NodeMTLSRequired,
			ClientMTLSRequired:             cfg.Cluster.ClientMTLSRequired,
			FailureKNodesToProbe:           cfg.Cluster.FailureKNodesToProbe,
			FailureProbeTimeout:            cfg.Cluster.FailureProbeTimeout,
			NodeFaultyAfter:                cfg.Cluster.NodeFaultyAfter,
		}

		rCfg.Cluster = options

		return rCfg, nil
	default:

		return nil, fmt.Errorf("invalid pkgConfig")
	}

}
