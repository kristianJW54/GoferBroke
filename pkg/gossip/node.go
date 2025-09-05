package gossip

import (
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"net"
	"os"
)

type Node struct {
	server        *cluster.GBServer
	nodeConfig    *cluster.GbNodeConfig
	clusterConfig *cluster.GbClusterConfig

	handlers map[string][]string // For now - will change once we have a delta handler type

}

//=====================================================================
// Gossip Node
//=====================================================================

func NewNodeFromConfig(config any, node any) (*Node, error) {
	var nodeConfig *cluster.GbNodeConfig
	var clusterConfig *cluster.GbClusterConfig
	var clusterSchema map[string]*cluster.ConfigSchema

	// --- Handle Node ---
	switch v := node.(type) {
	case *NodeConfig:
		nCfg, err := DeriveFromPkg(v)
		if err != nil {
			return nil, err
		}
		n, ok := nCfg.(*cluster.GbNodeConfig)
		if !ok {
			return nil, fmt.Errorf("expected *cluster.GbNodeConfig, got %T", nCfg)
		}
		nodeConfig = n

	case string:
		def := cluster.InitDefaultNodeConfig()

		if info, err := os.Stat(v); err == nil && !info.IsDir() {
			raw, err := os.ReadFile(v)
			if err != nil {
				return nil, fmt.Errorf("failed to read node config file: %w", err)
			}
			_, err = cluster.BuildConfigFromString(string(raw), def)
			if err != nil {
				return nil, fmt.Errorf("failed to parse node config file: %w", err)
			}
		} else {
			_, err := cluster.BuildConfigFromString(v, def)
			if err != nil {
				return nil, fmt.Errorf("failed to parse node config string: %w", err)
			}
		}
		nodeConfig = def
	}

	// --- Handle Cluster ---
	switch v := config.(type) {
	case *ClusterConfig:
		cCfg, err := DeriveFromPkg(v)
		if err != nil {
			return nil, err
		}
		c, ok := cCfg.(*cluster.GbClusterConfig)
		if !ok {
			return nil, fmt.Errorf("expected *cluster.GbClusterConfig, got %T", cCfg)
		}
		clusterConfig = c

	case string:
		def := cluster.InitDefaultClusterConfig()

		if info, err := os.Stat(v); err == nil && !info.IsDir() {
			raw, err := os.ReadFile(v)
			if err != nil {
				return nil, fmt.Errorf("failed to read cluster config file: %w", err)
			}
			sch, err := cluster.BuildConfigFromString(string(raw), def)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cluster config file: %w", err)
			}
			clusterSchema = sch
		} else {
			sch, err := cluster.BuildConfigFromString(v, def)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cluster config string: %w", err)
			}
			clusterSchema = sch
		}
		clusterConfig = def
	}

	// Validate seeds
	if len(clusterConfig.SeedServers) == 0 ||
		clusterConfig.SeedServers[0].Host == "" ||
		clusterConfig.SeedServers[0].Port == "" {
		return nil, fmt.Errorf("%w - seed host or port is missing", Errors.ClusterConfigErr)
	}

	fmt.Printf("seeds = %+v\n", clusterConfig.SeedServers)

	// Build schema (if not already parsed)
	if clusterSchema == nil {
		clusterSchema = cluster.BuildConfigSchema(clusterConfig)
	}

	// Create gossip server
	gbs, err := cluster.NewServer(
		nodeConfig.Name,
		clusterConfig,
		clusterSchema,
		nodeConfig,
		nodeConfig.Host,
		nodeConfig.Port,
		nodeConfig.ClientPort,
		net.ListenConfig{},
	)
	if err != nil {
		return nil, err
	}

	return &Node{
		server:        gbs,
		nodeConfig:    nodeConfig,
		clusterConfig: clusterConfig,
		handlers:      make(map[string][]string),
	}, nil
}

// Starting and stopping

func (n *Node) Start() {
	go n.server.StartServer()
}

func (n *Node) Stop() {
	n.server.Shutdown()
}
