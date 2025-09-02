package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectToSeedErrorEvent(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
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
		t.Errorf("error creating new server: %v", err)
	}

	// Start gbs2 first so that it dials an unresponsive seed to get error
	go gbs.StartServer()

	// Create a child context with timeout to catch if the error event doesn't pull us out
	ctx, cancel := context.WithTimeout(gbs.ServerContext, 5*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("❌ Shutdown did not happen in time (timeout expired)")
		}
	}

}
