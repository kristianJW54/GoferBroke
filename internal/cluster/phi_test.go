package cluster

import (
	"net"
	"testing"
	"time"
)

func TestPhiLive(t *testing.T) {

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

	gbs, err := NewServer("test-server", config, nil, nodeConfig, "localhost", "8081", "8080", lc)
	if err != nil {
		t.Fatal(err)
	}
	gbs2, err := NewServer("test-server", config, nil, nodeConfig, "localhost", "8082", "8083", lc)
	if err != nil {
		t.Fatal(err)
	}

	gbs.StartServer()
	time.Sleep(1 * time.Second)
	gbs2.StartServer()

	time.Sleep(5 * time.Second)
	gbs2.Shutdown()

	// Shutting down here will cause the phi score of test server 2 to rise for test server 1

	time.Sleep(10 * time.Second)
	gbs.ServerContext.Done()
	gbs.Shutdown()

	time.Sleep(1 * time.Second)

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()

}
