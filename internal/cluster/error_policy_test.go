package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestConnectToSeedErrorEvent(t *testing.T) {

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
			ClusterNetworkType: C_LOCAL,
		},
	}

	nodeConfig := &GbNodeConfig{
		Internal: &InternalOptions{},
	}

	gbs2, err := NewServer("test-server", config, nodeConfig, "localhost", "8082", "8083", lc)
	if err != nil {
		t.Errorf("TestServerRunningTwoNodes should not have returned an error - got %v", err)
		return
	}

	// Start gbs2 first so that it dials an unresponsive seed to get error
	gbs2.StartServer()

	// Create a child context with timeout to catch if the error event doesn't pull us out
	ctx, cancel := context.WithTimeout(gbs2.ServerContext, 5*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("❌ Shutdown did not happen in time (timeout expired)")
		} else {
			t.Log("✅ Shutdown happened successfully before timeout")
		}
	}

}
