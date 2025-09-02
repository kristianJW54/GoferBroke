package cluster

import (
	"net"
	"testing"
	"time"
)

// Test Ready ✅
func TestCreateClient(t *testing.T) {

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

	server, client := net.Pipe()
	defer func() {
		_ = server.Close()
		_ = client.Close()
	}()

	var c *gbClient

	c = gbs.createClient(client, CLIENT)

	time.Sleep(1 * time.Second)

	if !c.flags.isSet(WRITE_LOOP_STARTED) || !c.flags.isSet(READ_LOOP_STARTED) {
		t.Errorf("expected both read and write loops to have started")
	}

	if c.name == "" {
		t.Errorf("client should have a name")
	}

	if _, ok := gbs.clientStore.Load(c.name); !ok {
		t.Errorf("client should be stored in clientStore")
	}

	time.Sleep(1 * time.Second)

}

// TODO Need more client tests
