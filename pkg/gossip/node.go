package gossip

import (
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
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
	ID          uint32
	Host        string
	Port        string
	NetworkType string
	ClientPort  string
	config      *cluster.GbNodeConfig
}

func (nc *NodeConfig) InitConfig() error {

	netType, err := cluster.ParseNodeNetworkType(nc.NetworkType)
	if err != nil {
		return err
	}

	gbConfig := &cluster.GbNodeConfig{
		Name:        nc.Name,
		ID:          nc.ID,
		Host:        nc.Host,
		Port:        nc.Port,
		NetworkType: netType,
		ClientPort:  nc.ClientPort,
		Internal:    &cluster.InternalOptions{},
	}

	nc.config = gbConfig

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

	cNet, err := cluster.ParseClusterNetworkType(cc.NetworkType)
	if err != nil {
		return err
	}

	if len(cc.SeedServers) == 0 {
		return errors.New("no seed servers")
	}
	ss := make([]cluster.Seeds, len(cc.SeedServers))
	for i, server := range cc.SeedServers {
		ss[i].Port = server.SeedPort
		ss[i].Host = server.SeedHost
	}

	config := &cluster.GbClusterConfig{
		Name:        cc.Name,
		SeedServers: ss,
		Cluster: &cluster.ClusterOptions{
			ClusterNetworkType: cNet,
		},
	}

	cc.config = config

	return nil

}

//=====================================================================
// Gossip Node
//=====================================================================

// TODO may need to change the config to file paths to first parse report errors and then build new node??

func NewNodeFromConfig(config *ClusterConfig, node *NodeConfig) (*Node, error) {

	if len(config.SeedServers) == 0 ||
		config.SeedServers[0].SeedHost == "" ||
		config.SeedServers[0].SeedPort == "" {
		return nil, fmt.Errorf("%w - seed host or port is missing", Errors.ClusterConfigErr)
	}

	gbs, err := cluster.NewServer(
		node.Name,
		int(node.ID),
		config.config,
		node.config,
		node.Host,
		node.Port,
		node.ClientPort,
		net.ListenConfig{},
	)
	if err != nil {
		return nil, Errors.WrapGBError(Errors.ClusterConfigErr, err)
	}

	newNode := &Node{
		server:        gbs,
		nodeConfig:    node.config,
		clusterConfig: config.config,
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
