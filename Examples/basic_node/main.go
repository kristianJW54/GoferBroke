package main

import (
	"fmt"
	"github.com/kristianJW54/GoferBroke/pkg/gossip"
	"time"
)

// Basic Node using user specified config structs

// Example:

// A simple database application wants to distribute it's state and become highly available and fault-tolerant
// To do so it needs a protocol for replication such as Paxos or Raft but also a lightweight p2p gossip protocol
// to ensure availability and failure detection

// On each instance of the application a new gossip node will be created alongside the database instance
// let's call it node-1

// application-1 will be created and become the leader of a new database cluster and embedded with it will be node-1

// It will be created with a cluster wide config that the whole of the cluster will adhere and, its own internal node config
// which marks it as a unique node the cluster

func main() {

	// Declare cluster wide config - same for all application instances - changes would be gossiped to nodes and applied
	clusterConfig := &gossip.ClusterConfig{
		Name: "database_cluster",
		SeedServers: []gossip.Seeds{
			{
				SeedHost: "localhost",
				SeedPort: "8081",
			},
		},
		NetworkType: "LOCAL",
	}

	// In production this would be dynamically loaded from environment variables or such to ensure unique instances
	node1Config := &gossip.NodeConfig{
		Name:        "node",
		ID:          1,
		Host:        "localhost",
		Port:        "8081",
		NetworkType: "LOCAL",
		ClientPort:  "8083",
	}

	// Initialise our configs
	err := clusterConfig.InitConfig()
	if err != nil {
		panic(err)
	}
	err = node1Config.InitConfig()
	if err != nil {
		panic(err)
	}

	// Now we create our node-1
	node1, err := gossip.NewNodeFromConfig(clusterConfig, node1Config)
	if err != nil {
		panic(err)
	}

	node1.Start()

	// We give some time between node starts so that the two nodes have different time stamps and can defer correctly
	time.Sleep(1 * time.Second)

	// We now need to distribute the database and expand the cluster. To do so we create a new application instance with the same
	// Cluster config, but this time we dynamically create a new node config which will specify a new node

	// this will be called node-2

	// Application-2 will join the cluster as a follower (as per Raft protocol) but crucially for the gossip protocol, node-2 will be embedded and will reach out
	// to node-1 and the two will begin gossiping state and exchanging deltas

	// We now have a gossip cluster and the database application has two distributed instances which are keeping in sync and detecting each others
	// failures or state changes

	// the application can now continue with its purpose of replicating database logs from leaders to followers with a high degree of certainty that
	// nodes in the cluster are available

	// Create node-2 config which would be in a new application instance
	node2Config := &gossip.NodeConfig{
		Name:        "node",
		ID:          2,
		Host:        "localhost",
		Port:        "8082",
		NetworkType: "LOCAL",
		ClientPort:  "8083",
	}

	err = node2Config.InitConfig()
	if err != nil {
		panic(err)
	}

	// Now we create our node-2
	node2, err := gossip.NewNodeFromConfig(clusterConfig, node2Config)
	if err != nil {
		panic(err)
	}

	node2.Start()

	time.Sleep(10 * time.Second)
	node2.Stop()
	time.Sleep(1 * time.Second)
	node1.Stop()

	time.Sleep(1 * time.Second)

	fmt.Println("end of example")

}
