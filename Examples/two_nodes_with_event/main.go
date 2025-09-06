package main

import (
	"fmt"
	"github.com/kristianJW54/GoferBroke/pkg/gossip"
	"time"
)

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

	// Now we create our node-1
	node1, err := gossip.NewNodeFromConfig(c, n)
	if err != nil {
		panic(err)
	}

	node1.Start()

	time.Sleep(100 * time.Millisecond)

	// Now Node 2

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

	if _, err := node2.OnEvent(gossip.NewDeltaAdded, func(event gossip.Event) error {

		fmt.Printf("Handler received event: type=%v, message=%s\n", event.Type(), event.Message())

		delta, ok := event.Payload().(*gossip.DeltaAddedEvent)
		if !ok {
			return fmt.Errorf("event type error")
		}

		fmt.Printf("\n%s Received a new delta --> %s \nValue: %s\n", "node-1", delta.DeltaKey, string(delta.DeltaValue))

		return nil

	}); err != nil {
		panic(err)
	}

	if _, err := node2.OnEvent(gossip.DeltaUpdated, func(event gossip.Event) error {

		fmt.Printf("Handler received event: type=%v, message=%s\n", event.Type(), event.Message())

		delta, ok := event.Payload().(*gossip.DeltaUpdateEvent)
		if !ok {
			return fmt.Errorf("event type error")
		}

		fmt.Printf("\n%s Received updated delta --> %s \nValue: %s\n", "node-1", delta.DeltaKey, string(delta.CurrentValue))

		return nil

	}); err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Second)

	newDelta := gossip.CreateNewDelta("test", "key1", gossip.STRING, []byte("Hello there :)"))
	if err = node1.Add(newDelta); err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Second)

	updateDelta := gossip.CreateNewDelta("test", "key1", gossip.STRING, []byte("I am updated"))
	err = node1.Update("test", "key1", updateDelta)
	if err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Second)

	node2.Stop()
	time.Sleep(1 * time.Second)
	node1.Stop()

	time.Sleep(1 * time.Second)

	fmt.Println("end of example")

}
