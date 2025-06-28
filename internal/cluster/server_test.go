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

	node3Cfg := `
			Name = "node-3"
			Host = "localhost"
			Port = "8083"
			IsSeed = False`

	cfg := `Name = "default-local-cluster"
SeedServers = [
    {Host: "localhost", Port: "8081"},
]
Cluster {
    ClusterNetworkType = "LOCAL"
    NodeSelectionPerGossipRound = 1
}`

	gbs3, err := NewServerFromConfigString(node3Cfg, cfg)
	if err != nil {
		t.Error(err)
	}

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
	go gbs3.StartServer()

	time.Sleep(5 * time.Second)

	gbs2.Shutdown()
	gbs3.Shutdown()
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
	gbs3.logActiveGoRoutines()

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

func TestGossipSignal(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "localhost" // Use the full IP address
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
			ClusterNetworkType:          C_LOCAL,
			NodeSelectionPerGossipRound: 2,
		},
	}

	nodeConfig := &GbNodeConfig{
		Internal: &InternalOptions{},
	}

	gbs, _ := NewServer("test-server", config, nil, nodeConfig, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server2", config, nil, nodeConfig, "localhost", "8082", "8083", lc)
	gbs3, _ := NewServer("test-server3", config, nil, nodeConfig, "localhost", "8085", "8084", lc)

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()
	time.Sleep(1 * time.Second)
	go gbs3.StartServer()
	time.Sleep(6 * time.Second)

	gbs2.ServerContext.Done()
	gbs3.ServerContext.Done()
	gbs.ServerContext.Done()

	go gbs2.Shutdown()
	go gbs3.Shutdown()
	go gbs.Shutdown()

	time.Sleep(3 * time.Second)

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
	gbs3.logActiveGoRoutines()

}

//======================================================
// Testing gossip with different states for nodes
//======================================================

func TestGossipDifferentStates(t *testing.T) {

	// TODO make some different cluster map states for nodes about other nodes

	// Mock timestamps to use
	// 1735988400 = Sat Jan 04 2025 11:00:00 GMT+0000
	// 1735988535 = Sat Jan 04 2025 11:02:15 GMT+0000 --> + 2min 15sec
	// 1735988555 = Sat Jan 04 2025 11:02:35 GMT+0000 --> + 2min 35sec
	// 1735988625 = Sat Jan 04 2025 11:03:45 GMT+0000 --> + 3min 45sec
	// 1735988700 = Sat Jan 04 2025 11:05:00 GMT+0000 --> + 5min

	// TODO will need to change the participant timestamp and names to a standard

	lc := net.ListenConfig{}

	testConfig := &InternalOptions{
		IsTestMode:                            true,
		DisableGossip:                         false,
		DisableInitialiseSelf:                 false,
		DisableInternalGossipSystemUpdate:     true,
		DisableUpdateServerTimeStampOnStartup: true,
	}
	config := &GbClusterConfig{
		SeedServers: []*Seeds{
			{
				Host: "127.0.0.1",
				Port: "8081",
			},
		},
	}

	nodeConfig := &GbNodeConfig{
		Internal: testConfig,
	}

	gbs, _ := NewServer("test-server", config, nil, nodeConfig, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server", config, nil, nodeConfig, "localhost", "8082", "8080", lc)

	UpdateServerNameAndTime(t, gbs, 1735988400)
	UpdateServerNameAndTime(t, gbs2, 1735988401)

	// TODO Give each node a different cluster map to converge on and gossip about

	go gbs.StartServer()
	go gbs2.StartServer()

	time.Sleep(5 * time.Second)

	go gbs.Shutdown()
	go gbs2.Shutdown()

	time.Sleep(1 * time.Second)

}

func UpdateServerNameAndTime(t testing.TB, node *GBServer, time uint64) {
	t.Helper()

	node.ServerID.timeUnix = time
	node.ServerName = node.String()

}
