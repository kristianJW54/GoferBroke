package cluster

import (
	"bytes"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
	"net"
	"testing"
	"time"
)

func TestServerNameLengthError(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbClusterConfig{
		SeedServers: []*Seeds{
			{
				Host: ip,
				Port: port,
			},
		},
		Cluster: &ClusterOptions{
			ClusterNetworkType: C_LOCAL,
		},
	}

	nodeConfig := &GbNodeConfig{
		Internal: &InternalOptions{},
	}

	_, err := NewServer("test-server", config, nil, nodeConfig, "localhost", "8081", "8080", lc)
	if err == nil {
		t.Errorf("TestServerNameLengthError should have returned an error")
	}

	log.Printf("error = %v", err)

}

func TestServerRunningTwoNodes(t *testing.T) {

	clusterPath := "../../Configs/cluster/default_cluster_config.conf"

	seedFilePath := "../../Configs/node/basic_seed_config.conf"
	nodeFilePath := "../../Configs/node/basic_node_config.conf"

	//	node3Cfg := `
	//			Name = "node-3"
	//			Host = "localhost"
	//			Port = "8083"
	//			IsSeed = False`
	//
	//	cfg := `Name = "default-local-cluster"
	//SeedServers = [
	//    {Host: "localhost", Port: "8081"},
	//]
	//Cluster {
	//    ClusterNetworkType = "LOCAL"
	//    NodeSelectionPerGossipRound = 1
	//}`
	//
	//	gbs3, err := NewServerFromConfigString(node3Cfg, cfg)
	//	if err != nil {
	//		t.Error(err)
	//	}

	gbs, err := NewServerFromConfigFile(seedFilePath, clusterPath)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	gbs2, err := NewServerFromConfigFile(nodeFilePath, clusterPath)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()
	//go gbs3.StartServer()

	time.Sleep(5 * time.Second)

	gbs2.Shutdown()
	//gbs3.Shutdown()
	//time.Sleep(1 * time.Second)
	gbs.Shutdown()
	time.Sleep(1 * time.Second)

	//for k, v := range gbs.clusterMap.participants {
	//	fmt.Printf("name = %s\n", k)
	//	for k, value := range v.keyValues {
	//		fmt.Printf("%s-%+s\n", k, value.Value)
	//	}
	//}

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
	//gbs3.logActiveGoRoutines()

}

func TestRandomNodeSelection(t *testing.T) {

	partArray := []string{
		"part-1",
		"part-2",
		"part-3",
		"part-4",
	}

	notToGossip := make(map[string]interface{})

	notToGossip["part-1"] = struct{}{}
	notToGossip["part-3"] = struct{}{}

	ns := 2

	indexes, err := generateRandomParticipantIndexesForGossip(partArray, ns, notToGossip)
	if err != nil {
		t.Errorf("%v", err)
	}

	for _, index := range indexes {
		log.Printf("part selected = %s", partArray[index])
	}

}

func TestServerShutDownConfigFail(t *testing.T) {

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True	
				}

`

	node2Cfg := `
				Name = "node-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = True	
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	cfg2 := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 2
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}
	gbs2, err := NewServerFromConfigString(node2Cfg, cfg2)
	if err != nil {
		t.Errorf("%v", err)
	}

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()

	select {
	case err := <-gbs2.fatalErrorCh:
		if err != nil {
			he := Errors.HandleError(err, func(gbErrors []*Errors.GBError) error {

				for _, gbError := range gbErrors {

					if gbError.Code == Errors.CONFIG_CHECKSUM_FAIL_CODE {
						return gbError
					}
				}
				return nil

			})

			if he == nil {
				t.Errorf("expected to have found fatal error %d - got nil instead", Errors.CONFIG_CHECKSUM_FAIL_CODE)
			}

		} else {
			t.Errorf("fatalErrorCh received nil error")
		}
	case <-time.After(5 * time.Second):
		t.Error("Timed out waiting for fatal error")
	}

	gbs.Shutdown()

}

func TestServerShutDownConfigUpdate(t *testing.T) {

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True	
				}

`

	node2Cfg := `
				Name = "node-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = True	
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	gbs2, err := NewServerFromConfigString(node2Cfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	gbs.StartServer()
	time.Sleep(1 * time.Second)
	gbs.gbClusterConfig.Name = "new-cluster-name"
	time.Sleep(1 * time.Second)
	gbs2.StartServer()

	select {
	case err := <-gbs2.fatalErrorCh:
		if err != nil {
			log.Printf("%v", err)
		}
	case err := <-gbs.fatalErrorCh:
		if err != nil {
			log.Printf("%v", err)
		}
	case <-time.After(5 * time.Second):
		gbs.Shutdown()
		gbs2.Shutdown()
	}

	// gbs2 config check should now be updated to gbs config

}

func TestServerUpdateSelfConfig(t *testing.T) {

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True	
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	gbs.Shutdown()

	clusterKey := "Name"
	newValue := "new-cluster-name"

	deltaUpdate := &Delta{
		KeyGroup:  CONFIG_DKG,
		Key:       clusterKey,
		Version:   1,
		ValueType: D_STRING_TYPE,
		Value:     []byte(newValue),
	}

	// Now we change a config in the cluster

	if _, exists := gbs.clusterMap.participants[gbs.ServerName].keyValues["config:Name"]; exists {
		err := gbs.updateClusterConfigDeltaAndSelf(deltaUpdate.Key, deltaUpdate)
		if err != nil {
			t.Errorf("%v", err)
		}
		d := gbs.clusterMap.participants[gbs.ServerName].keyValues["config:Name"]

		if bytes.Compare(d.Value, deltaUpdate.Value) != 0 {
			t.Errorf("wanted %s got %s", newValue, d.Value)
		}

	}

}

//======================================================
// Testing handling dead nodes

// Test Ready For Use ✅
func TestServerReachingOutToDeadNodeAndHandling(t *testing.T) {

	// Seed server should identify the dead node - attempt 3 gossip retries and then
	// move to notToGossip and delete from nodeConnStore
	// it should also clear the node map and add in a delete delta

	// Our phi warm up means we have a higher threshold at 12 and after 30 records we then move
	// the threshold down to 9.5 the more we warm up and get close to window size
	// therefore we must wait at least 30 seconds until we can hit the threshold
	// and then a further amount of time for us to hit our retries

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = False
					DefaultLoggerEnabled = False
				}

`

	node2Cfg := `
				Name = "node-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = False
					DefaultLoggerEnabled = False
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}
	gbs2, err := NewServerFromConfigString(node2Cfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()

	time.Sleep(31 * time.Second)

	gbs2.Shutdown()

	time.Sleep(15 * time.Second)

	gbs.Shutdown()
	time.Sleep(1 * time.Second)

	if _, exists := gbs.notToGossipNodeStore[gbs2.ServerName]; !exists {
		t.Errorf("expected to find %s in notToGossipNodeStore", gbs2.ServerName)
	}

	if gbs.clusterMap.participants[gbs2.ServerName].paDetection.reachAttempts != 3 {
		t.Errorf("expected to have retried dead node 3 times got %d", gbs.clusterMap.participants[gbs2.ServerName].paDetection.reachAttempts)
	}

	if len(gbs.clusterMap.participants[gbs2.ServerName].keyValues) != 1 {
		t.Errorf("expected to have only 1 delta in %s's cluster map", gbs2.ServerName)
	}

	//for k, v := range gbs.clusterMap.participants {
	//	fmt.Printf("name = %s\n", k)
	//	for k, value := range v.keyValues {
	//		fmt.Printf("%s-%+s\n", k, value.Value)
	//	}
	//}

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()

}

// Test Ready For Use ✅
// May need to find a way not to wait that long though...
func TestNode3ReceivingDeadNodeDelta(t *testing.T) {

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = False
					DefaultLoggerEnabled = False
				}

`

	node2Cfg := `
				Name = "node-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = False
					DefaultLoggerEnabled = False
				}

`

	node3Cfg := `
				Name = "node-3"
				Host = "localhost"
				Port = "8083"
				IsSeed = False
				Internal {
					DisableGossip = False
					DefaultLoggerEnabled = False
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}
	gbs2, err := NewServerFromConfigString(node2Cfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}
	gbs3, err := NewServerFromConfigString(node3Cfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()
	time.Sleep(1 * time.Second)
	go gbs3.StartServer()

	time.Sleep(30 * time.Second)

	gbs2.Shutdown()

	time.Sleep(30 * time.Second)

	gbs.Shutdown()
	gbs3.Shutdown()
	time.Sleep(1 * time.Second)

	for k, v := range gbs3.clusterMap.participants {
		fmt.Printf("name = %s\n\n", k)
		for k, value := range v.keyValues {
			fmt.Printf("%s-%+s\n", k, value.Value)
		}
	}

	if len(gbs3.clusterMap.participants[gbs2.ServerName].keyValues) != 1 {
		t.Errorf("expected to have only 1 delta in %s's cluster map", gbs2.ServerName)
	}

	if _, exists := gbs3.clusterMap.participants[gbs2.ServerName].keyValues[MakeDeltaKey(SYSTEM_DKG, _DEAD_)]; !exists {
		t.Errorf("expected to find dead delta for %s in gbs3 cluster map", gbs2.ServerName)
	}

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
	gbs3.logActiveGoRoutines()

}

//======================================================
// Testing gossip with different states for nodes
//======================================================
