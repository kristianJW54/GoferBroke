package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestBasicServerLog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ring := newLogBuffer(100)
	console := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	async := newSlogLogger(ctx, ring, console, 256)

	logger := slog.New(async)

	slog.SetDefault(logger)

	mod := "test"

	// Write logs
	logger.Info("Test log message", "module", mod, "step", 1)
	logger.Warn("Something happened", "code", 123)

	go func() {
		logger.Info("Logging inside go-routine lol")
	}()

	go func() {
		logger.Info("Logging inside go-routine lol AGAIN!")
	}()

	// Optional: wait briefly or rely on async.Close() drain
	time.Sleep(100 * time.Millisecond)

	// Snapshot and assert
	snapshot := ring.Snapshot()

	if len(snapshot) < 2 {
		t.Errorf("expected at least 2 log entries, got %d", len(snapshot))
	}

	for _, snap := range snapshot {
		fmt.Printf("snapshot -----> %s\n", snap)
	}

}

func TestServerStartLog(t *testing.T) {

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

	//
	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	time.Sleep(500 * time.Millisecond)

	gbs.StartServer()
	time.Sleep(2 * time.Second)

	entries := gbs.logRingBuffer.Snapshot()
	if len(entries) < 1 {
		t.Errorf("expected at least 1 log entry, got %d", len(entries))
	}

	fmt.Printf("\n")

	for _, e := range entries {
		fmt.Printf("entry from snapshot = %s\n", e)
	}

	fmt.Printf("\n")

	gbs.Shutdown()
	gbs.logHandler.Close()
	fmt.Printf("Test Stopped\n")

}
