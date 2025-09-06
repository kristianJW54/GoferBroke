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
	c, err := gossip.BuildClusterConfig("default-cluster", func(config *gossip.ClusterConfig) error {

		config.SeedServers = []*gossip.Seeds{
			{Host: "localhost", Port: "8081"},
		}

		config.Cluster.ClusterNetworkType = gossip.C_LOCAL

		return nil

	})
	if err != nil {
		fmt.Println(err)
	}

	// In production this would be dynamically loaded from environment variables or such to ensure unique instances
	n, err := gossip.BuildNodeConfig("node", "localhost:8081", func(cfg *gossip.NodeConfig) (*gossip.NodeConfig, error) {

		cfg.NetworkType = gossip.LOCAL
		cfg.IsSeed = true
		cfg.ClientPort = "8083"

		return cfg, nil

	})

	node1, err := gossip.NewNodeFromConfig(c, n)
	if err != nil {
		panic(err)
	}

	if _, err := node1.OnEvent(gossip.ParticipantMarkedDead, func(event gossip.Event) error {

		fmt.Printf("Handler received event: type=%v, message=%s\n", event.Type(), event.Message())

		_, ok := event.Payload().(*gossip.ParticipantFaulty)
		if !ok {
			return fmt.Errorf("event type error")
		}

		fmt.Printf("participant marked dead")

		return nil

	}); err != nil {
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
	n2, err := gossip.BuildNodeConfig("node2", "localhost:8082", func(cfg *gossip.NodeConfig) (*gossip.NodeConfig, error) {

		cfg.NetworkType = gossip.LOCAL
		cfg.IsSeed = false
		cfg.ClientPort = "8084"

		return cfg, nil

	})

	// Now we create our node-2
	node2, err := gossip.NewNodeFromConfig(c, n2)
	if err != nil {
		panic(err)
	}

	node2.Start()

	time.Sleep(5 * time.Second)
	node2.Stop()
	time.Sleep(20 * time.Second)
	node1.Stop()

	time.Sleep(1 * time.Second)

	fmt.Println("end of example")

}
