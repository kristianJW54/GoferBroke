package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// Delta Header should be --> Command[V], MessageLength[0 0], KeyLength[0], ValueLength [0 0]

func TestClientDelta(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				ClientPort = "6000"
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

	go gbs.StartServer()

	time.Sleep(1 * time.Second)

	// Dial the TCP server
	conn, err := net.Dial("tcp", "localhost:6000")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}

	time.Sleep(1 * time.Second)

	conn.Write([]byte("PING\r\n"))

	time.Sleep(100 * time.Millisecond)

	//Check the cluster map is the same

	gbs.Shutdown()

	time.Sleep(100 * time.Millisecond)

	gbs.logActiveGoRoutines()

}
