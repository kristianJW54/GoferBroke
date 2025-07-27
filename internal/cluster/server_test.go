package cluster

import (
	"bytes"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"strings"
	"testing"
	"time"
)

// Test Ready ✅
func TestServerNameLengthError(t *testing.T) {

	nodeCfg := `
				Name = "iamaverylongnamewhichshouldneverbeallowedtobeconfiguredbecauseigetauuidanywaysonowimjustbeinggreedy"
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

	_, err := NewServerFromConfigString(nodeCfg, cfg)
	if err == nil {
		t.Errorf("expected error of exceed name length")
		return
	}
}

// Test Ready ✅
func TestNewServerFunction(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
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
		t.Errorf("error creating new server: %v", err)
	}

	if gbs.addr != "localhost:8081" {
		t.Errorf("expected bound TCP address to be localhost:8081")
	}

	if !gbs.isSeed {
		t.Errorf("expected isSeed to be true")
	}

	if gbs.gbClusterConfig.Cluster.ClusterNetworkType != C_LOCAL {
		t.Errorf("expected ClusterNetworkType to be 4 got %d", gbs.gbClusterConfig.Cluster.ClusterNetworkType)
	}

}

// Test Ready ✅
func TestWrongServerMode(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"

				// We should error here because we are saying we are not a seed but we have the same seed addr in cluster config
				IsSeed = False

				Internal { 
					DisableGossip = True
					DisableStartupMessage = True
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

	_, err := NewServerFromConfigString(nodeCfg, cfg)
	if err == nil {
		t.Errorf("expected an error")
	} else {
		if !strings.Contains(err.Error(), "server is NOT configured as a seed, but matches a listed seed address — change config to mark it as a seed OR use a different address") {
			t.Errorf("expected error to contain 'invalid server mode', got: %v", err)
		}
	}

}

// Test Ready ✅
func TestServerStartup(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
					LogToBuffer = False
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
		t.Errorf("error creating new server: %v", err)
	}

	gbs.StartServer()

	time.Sleep(100 * time.Millisecond)

	if gbs.nodeListener == nil {
		t.Errorf("node listener was not created")
	}
	if gbs.ServerName == "" {
		t.Errorf("server name was not created")
	}
	if gbs.flags.isSet(SHUTTING_DOWN) {
		t.Errorf("expected server to not be shutting down during start")
	}

	gbs.Shutdown()

}

func TestDialSeed(t *testing.T) {}

func TestInitParticipant(t *testing.T) {}

// Live Unit Tests

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

	for k, v := range gbs.clusterMap.participants {
		fmt.Printf("name = %s\n", k)
		for k, value := range v.keyValues {
			fmt.Printf("%s-%+s\n", k, value.Value)
		}
	}

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
	//gbs3.logActiveGoRoutines()

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
			fmt.Printf("%v\n", err)
		}
	case err := <-gbs.fatalErrorCh:
		if err != nil {
			fmt.Printf("%v\n", err)
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
// Testing gossip with different states for nodes
//======================================================
