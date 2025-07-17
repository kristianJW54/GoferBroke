package gossip

import (
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"log"
	"net"
)

type Node struct {
	server        *cluster.GBServer
	nodeConfig    *cluster.GbNodeConfig
	clusterConfig *cluster.GbClusterConfig

	handlers map[string][]string // For now - will change once we have a delta handler type

}

//=====================================================================
// Node Config
//=====================================================================

type NodeConfig struct {
	Name        string
	Host        string
	Port        string
	NetworkType string
	IsSeed      bool
	ClientPort  string
	config      *cluster.GbNodeConfig
}

func (nc *NodeConfig) InitConfig() error {

	netType, err := cluster.ParseNodeNetworkTypeFromString(nc.NetworkType)
	if err != nil {
		return err
	}

	cfg := cluster.InitDefaultNodeConfig()

	cfg.Name = nc.Name
	cfg.Host = nc.Host
	cfg.Port = nc.Port
	cfg.ClientPort = nc.ClientPort
	cfg.IsSeed = nc.IsSeed
	cfg.NetworkType = netType

	nc.config = cfg

	return nil

}

// Methods for config internal control
// May need locks if we are modifying internal state during live server...?

func (n *Node) EnableDebug() {
	n.nodeConfig.Internal.DebugMode = true
}

func (n *Node) DisableDebug() {
	n.nodeConfig.Internal.DebugMode = false
}

func (n *Node) EnableGoRoutineTracking() {
	n.nodeConfig.Internal.GoRoutineTracking = true
}

func (n *Node) DisableGoRoutineTracking() {
	n.nodeConfig.Internal.GoRoutineTracking = false
}

//=====================================================================
// Cluster Config
//=====================================================================

type ClusterConfig struct {
	Name        string
	SeedServers []Seeds
	NetworkType string
	config      *cluster.GbClusterConfig
}

type Seeds struct {
	SeedHost string
	SeedPort string
}

func (cc *ClusterConfig) InitConfig() error {

	cNet, err := cluster.ParseClusterNetworkTypeFromString(cc.NetworkType)
	if err != nil {
		return err
	}

	cfg := cluster.InitDefaultClusterConfig()

	if len(cc.SeedServers) == 0 {
		return errors.New("no seed servers")
	}
	ss := make([]*cluster.Seeds, len(cc.SeedServers))
	for i, server := range cc.SeedServers {
		ss[i] = &cluster.Seeds{
			Host: server.SeedHost,
			Port: server.SeedPort,
		}
	}

	cfg.Name = cc.Name
	cfg.Cluster.ClusterNetworkType = cNet
	cfg.SeedServers = ss

	cc.config = cfg

	log.Printf("seeds = %+v", cc.SeedServers)

	return nil

}

//=====================================================================
// Gossip Node
//=====================================================================

// TODO may need to change the config to file paths to first parse report errors and then build new node??

func NewNodeFromConfig(Config *ClusterConfig, node *NodeConfig) (*Node, error) {

	if len(Config.SeedServers) == 0 ||
		Config.SeedServers[0].SeedHost == "" ||
		Config.SeedServers[0].SeedPort == "" {
		return nil, fmt.Errorf("%w - seed host or port is missing", Errors.ClusterConfigErr)
	}

	err := Config.InitConfig()
	if err != nil {
		return nil, err
	}

	err = node.InitConfig()
	if err != nil {
		return nil, err
	}

	sch := cluster.BuildConfigSchema(Config.config)

	gbs, err := cluster.NewServer(
		node.Name,
		Config.config,
		sch,
		node.config,
		node.Host,
		node.Port,
		node.ClientPort,
		net.ListenConfig{},
	)
	if err != nil {
		//return nil, Errors.ChainGBErrorf(Errors.ClusterConfigErr, err, "")
		return nil, err
	}

	newNode := &Node{
		server:        gbs,
		nodeConfig:    node.config,
		clusterConfig: Config.config,
		handlers:      make(map[string][]string),
	}

	return newNode, nil

}

// Starting and stopping

func (n *Node) Start() {
	go n.server.StartServer()
}

func (n *Node) Stop() {
	n.server.Shutdown()
}
