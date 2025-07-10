package cluster

import (
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

	for k, v := range gbs.clusterMap.participants {
		log.Printf("name = %s", k)
		for k, value := range v.keyValues {
			log.Printf("%s-%+s", k, value.Value)
		}
	}

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

	ns := 2

	indexes, err := generateRandomParticipantIndexesForGossip(partArray, ns, partArray[0])
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

	log.Printf("name -> %s", gbs.name)
	log.Printf("name -> %s", gbs2.name)

	gbs.StartServer()
	time.Sleep(1 * time.Second)
	gbs2.StartServer()
	time.Sleep(5 * time.Second)

	// TODO Need to setup our config checks now

}

//======================================================
// Testing gossip with different states for nodes
//======================================================
