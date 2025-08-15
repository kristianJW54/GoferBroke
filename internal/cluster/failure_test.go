package cluster

import (
	"encoding/json"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"strings"
	"testing"
	"time"
)

// Test Ready ✅
func TestIndirectProbeErrorForTwoNodes(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
					LogToBuffer = True
				}

`

	nodeCfg2 := `
				Name = "t-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
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

	gbs2, err := NewServerFromConfigString(nodeCfg2, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs2.StartServer()

	time.Sleep(4 * time.Second)
	gbs2.Shutdown()
	time.Sleep(2 * time.Second)

	gbs.Shutdown()

	for _, snap := range gbs.jsonBuffer.Snapshot() {
		var rec struct {
			Time  string `json:"time"`
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if err := json.Unmarshal(snap, &rec); err != nil {
			t.Fatalf("bad log json: %v\n%s", err, string(snap))
		}
		if strings.EqualFold(rec.Level, "WARN") {
			// Test Assert here
			if !strings.Contains(rec.Msg, Errors.IndirectProbeErr.ErrMsg) {
				t.Errorf("[WARN] log does not contain [%s]", Errors.IndirectProbeErr.ErrMsg)
			}
			break
		}
	}

}

func TestMarkSuspectForTwoNodes(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
					LogToBuffer = True
				}

`

	nodeCfg2 := `
				Name = "t-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = True
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

	gbs2, err := NewServerFromConfigString(nodeCfg2, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs2.StartServer()

	time.Sleep(4 * time.Second)
	gbs2.gossip.gossipControlChannel <- false
	time.Sleep(3 * time.Second)
	gbs2.gossip.gossipControlChannel <- true
	gbs2.gossip.gossSignal.Broadcast()

	time.Sleep(4 * time.Second)

	gbs2.Shutdown()
	gbs.Shutdown()

}
