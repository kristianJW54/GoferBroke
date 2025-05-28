package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestPhiCalc(t *testing.T) {

	tests := []struct {
		name      string
		window    []int64
		value     float64
		threshold float64
		failed    bool
	}{
		{
			name:      "normal node",
			window:    []int64{1, 2, 1, 1, 1, 1, 1, 1, 1, 1},
			value:     1,
			threshold: 3,
			failed:    false,
		},
		{
			name:      "node delay in middle",
			window:    []int64{1, 1, 2, 1, 10, 5, 2, 1, 1, 1},
			value:     1,
			threshold: 3,
			failed:    false,
		},
		{
			name:      "failed node",
			window:    []int64{1, 1, 1, 2, 3, 4, 5, 6, 7, 10},
			value:     14,
			threshold: 3,
			failed:    true,
		},
		{
			name:      "warm up phase",
			window:    []int64{1, 0},
			value:     2,
			threshold: 3,
			failed:    false,
		},
	}

	// range through tests
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			phi := phi(tt.value, tt.window)

			var result bool

			if phi > tt.threshold {
				result = true
			} else {
				result = false
			}

			if result != tt.failed {
				t.Errorf("wanted %v, got %v", tt.failed, result)
			}

			fmt.Printf("ϕ = %.2f\n", phi)

		})

	}
}

func TestPhiLive(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbClusterConfig{
		SeedServers: []Seeds{
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

	gbs, err := NewServer("test-server", 1, config, nodeConfig, "localhost", "8081", "8080", lc)
	if err != nil {
		t.Fatal(err)
	}
	gbs2, err := NewServer("test-server", 2, config, nodeConfig, "localhost", "8082", "8083", lc)
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
