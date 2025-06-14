package main

import (
	"fmt"
	"github.com/kristianJW54/GoferBroke/pkg/gossip"
	"time"
)

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

	time.Sleep(1 * time.Second)

	if _, err := node1.OnEvent(gossip.NewDeltaAdded, func(event gossip.Event) error {

		fmt.Printf("Handler received event: type=%v, message=%s", event.Type(), event.Message())

		delta, ok := event.Payload().(*gossip.DeltaAddedEvent)
		if !ok {
			return fmt.Errorf("event type error")
		}

		fmt.Printf("\n%s Received a new delta --> %s \nValue: %s\n", "node-1", delta.DeltaKey, string(delta.DeltaValue))

		return nil

	}); err != nil {
		panic(err)
	}

	// Now Node 2

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

	time.Sleep(5 * time.Second)

	newDelta := gossip.CreateNewDelta("test", "key1", gossip.STRING, []byte("Hello there :)"))
	if err = node2.Add(newDelta); err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Second)
	node2.Stop()
	time.Sleep(1 * time.Second)
	node1.Stop()

	time.Sleep(1 * time.Second)

	fmt.Println("end of example")

}
