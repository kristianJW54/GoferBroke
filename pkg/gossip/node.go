package gossip

import (
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Cluster"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"net"
)

type Node struct {
	server        *Cluster.GBServer
	nodeConfig    *Cluster.GbNodeConfig
	clusterConfig *Cluster.GbClusterConfig

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
	config      *Cluster.GbNodeConfig
}

func (nc *NodeConfig) InitConfig() error {

	netType, err := Cluster.ParseNodeNetworkType(nc.NetworkType)
	if err != nil {
		return err
	}

	gbConfig := &Cluster.GbNodeConfig{
		Name:        nc.Name,
		ID:          nc.ID,
		Host:        nc.Host,
		Port:        nc.Port,
		NetworkType: netType,
		ClientPort:  nc.ClientPort,
		Internal:    &Cluster.InternalOptions{},
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
	config      *Cluster.GbClusterConfig
}

type Seeds struct {
	SeedHost string
	SeedPort string
}

func (cc *ClusterConfig) InitConfig() error {

	cNet, err := Cluster.ParseClusterNetworkType(cc.NetworkType)
	if err != nil {
		return err
	}

	if len(cc.SeedServers) == 0 {
		return errors.New("no seed servers")
	}
	ss := make([]Cluster.Seeds, len(cc.SeedServers))
	for i, server := range cc.SeedServers {
		ss[i].SeedPort = server.SeedPort
		ss[i].SeedHost = server.SeedHost
	}

	config := &Cluster.GbClusterConfig{
		Name:        cc.Name,
		SeedServers: ss,
		Cluster: &Cluster.ClusterOptions{
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

	gbs, err := Cluster.NewServer(
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
