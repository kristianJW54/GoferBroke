package cluster

import (
	"context"
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

	var want struct {
		Msg string
	}

	for _, snap := range gbs.jsonBuffer.Snapshot() {
		var rec struct {
			Time  string `json:"time"`
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}

		if err := json.Unmarshal(snap, &rec); err != nil {
			t.Fatalf("bad log json: %v\n%s", err, string(snap))
		}
		if rec.Level == "ERROR" {
			// Test Assert here
			if strings.Contains(rec.Msg, Errors.IndirectProbeErr.ErrMsg) {
				want = struct{ Msg string }{Msg: rec.Msg}
			}
			break
		}
	}

	if want.Msg == "" {
		t.Errorf("expected to get message -> %s", Errors.IndirectProbeErr.ErrMsg)
	}

}

// Test Ready ✅
func TestProbeCommand(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	nodeCfg2 := `
				Name = "t-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	nodeCfg3 := `
				Name = "t-3"
				Host = "localhost"
				Port = "8083"
				IsSeed = False
				Internal {
					DisableGossip = True
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

	gbs3, err := NewServerFromConfigString(nodeCfg3, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(100 * time.Millisecond)
	go gbs2.StartServer()
	go gbs3.StartServer()

	time.Sleep(2 * time.Second)

	// node 2 want to probe node 3 through node 1
	name := []byte(gbs3.ServerName)
	name = append(name, '\r', '\n')

	reqID, _ := gbs2.acquireReqID()

	pay, err := prepareRequest(name, 1, PROBE, reqID, uint16(0))
	if err != nil {
		t.Errorf("%v", err)
	}

	ctx, cancel := context.WithTimeout(gbs2.ServerContext, 7*time.Second)
	defer cancel()

	client, exists, err := gbs2.getNodeConnFromStore(gbs.ServerName)
	if err != nil || !exists {
		t.Errorf("%v - or doesn't exist", err)
	}

	resp := client.qProtoWithResponse(ctx, reqID, pay, true)

	rsp, err := client.waitForResponseAndBlock(resp)
	if err != nil {
		t.Errorf("%v", err)
	}

	time.Sleep(1 * time.Second)

	gbs.Shutdown()
	gbs2.Shutdown()
	gbs3.Shutdown()

	time.Sleep(100 * time.Millisecond)

	if rsp.msg == nil {
		t.Errorf("expected to get an ok resp - got nil response instead")
	}

}

// Test Ready ✅
func TestSendProbeFunction(t *testing.T) {

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

	nodeCfg3 := `
				Name = "t-3"
				Host = "localhost"
				Port = "8083"
				IsSeed = False
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	nodeCfg4 := `
				Name = "t-4"
				Host = "localhost"
				Port = "8084"
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

	gbs3, err := NewServerFromConfigString(nodeCfg3, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	gbs4, err := NewServerFromConfigString(nodeCfg4, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs2.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs3.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs4.StartServer()

	time.Sleep(5 * time.Second)

	// Here we want to run two tests - one for testing that single helper selected can successfully probe the target
	// Second that two helpers can probe in parallel the target and respect the context and channels
	// Then we test no helpers for a quick return

	ctx, cancel := context.WithTimeout(gbs.ServerContext, 1*time.Second)
	defer cancel()
	err = gbs.handleIndirectProbe(ctx, gbs4.ServerName)
	if err != nil {
		t.Errorf("%v", err)
	}

	time.Sleep(1 * time.Second)

	gbs.Shutdown()
	gbs2.Shutdown()
	gbs3.Shutdown()
	gbs4.Shutdown()

}

// Test Ready ✅
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
	gbs2.gossip.gossipControlChannel <- false
	time.Sleep(1 * time.Second)

	part, exist := gbs.clusterMap.participants[gbs2.ServerName]
	if !exist {
		t.Errorf("participant %s not found in clusterMap", gbs2.ServerName)
	}
	if part.f.state != SUSPECTED {
		t.Errorf("participant %s not suspect", gbs2.ServerName)
	}

	gbs2.Shutdown()
	gbs.Shutdown()

}

// Test Ready ✅
func TestRefuteSuspectForTwoNodes(t *testing.T) {

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
	gbs2.gossip.gossipControlChannel <- false
	time.Sleep(3 * time.Second)
	gbs2.gossip.gossipControlChannel <- true
	gbs2.gossip.gossSignal.Broadcast()

	time.Sleep(4 * time.Second)

	gbs2.Shutdown()
	gbs.Shutdown()

	var want struct {
		Msg string
	}

	for _, snap := range gbs.jsonBuffer.Snapshot() {
		var rec struct {
			Time  string `json:"time"`
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}

		if err := json.Unmarshal(snap, &rec); err != nil {
			t.Fatalf("bad log json: %v\n%s", err, string(snap))
		}
		if rec.Level == "WARN" {
			// Test Assert here
			if strings.Contains(rec.Msg, "we have been refuted") {
				want = struct{ Msg string }{Msg: rec.Msg}
			}
			break
		}
	}

	if want.Msg == "" {
		t.Errorf("expected to get message -> we have been refuted")
	}

}

// Test Ready ✅
func TestProbeSuccessForThreeNodes(t *testing.T) {

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

	nodeCfg3 := `
				Name = "t-3"
				Host = "localhost"
				Port = "8083"
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

	gbs3, err := NewServerFromConfigString(nodeCfg3, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs2.StartServer()
	go gbs3.StartServer()

	time.Sleep(4 * time.Second)
	gbs2.gossip.gossipControlChannel <- false
	time.Sleep(1 * time.Second)
	// Node 2 stops gossip 'slow to respond' probe commands will still work so should be able to stop being reported as suspect
	gbs2.gossip.gossipControlChannel <- true
	gbs2.gossip.gossSignal.Broadcast()

	time.Sleep(1 * time.Second)

	gbs2.Shutdown()
	gbs3.Shutdown()
	gbs.Shutdown()

	if d, exists := gbs.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(FAILURE_DKG, gbs2.ServerName)]; exists {
		if d.Value[0] == SUSPECTED {
			t.Errorf("node %s should not be marked as suspected", gbs2.ServerName)
		}
	} else {
		t.Errorf("node %s should exist in our clustermap", gbs2.ServerName)
	}

	if d, exists := gbs3.clusterMap.participants[gbs3.ServerName].keyValues[MakeDeltaKey(FAILURE_DKG, gbs2.ServerName)]; exists {
		if d.Value[0] == SUSPECTED {
			t.Errorf("node %s should not be marked as suspected", gbs2.ServerName)
		}
	} else {
		t.Errorf("node %s should exist in our clustermap", gbs2.ServerName)
	}

}

// Test Ready ✅
func TestProbeFailForThreeNodes(t *testing.T) {

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

	nodeCfg3 := `
				Name = "t-3"
				Host = "localhost"
				Port = "8083"
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

	gbs3, err := NewServerFromConfigString(nodeCfg3, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs2.StartServer()
	go gbs3.StartServer()

	time.Sleep(5 * time.Second)
	gbs2.Shutdown()
	time.Sleep(3 * time.Second)

	gbs3.Shutdown()
	gbs.Shutdown()

	if d, exists := gbs.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(FAILURE_DKG, gbs2.ServerName)]; exists {
		if d.Value[0] != SUSPECTED {
			t.Errorf("node %s should be marked as suspected", gbs2.ServerName)
		}
	} else {
		t.Errorf("node %s should exist in our clustermap", gbs2.ServerName)
	}

	if d, exists := gbs3.clusterMap.participants[gbs3.ServerName].keyValues[MakeDeltaKey(FAILURE_DKG, gbs2.ServerName)]; exists {
		if d.Value[0] != SUSPECTED {
			t.Errorf("node %s should be marked as suspected", gbs2.ServerName)
		}
	} else {
		t.Errorf("node %s should exist in our clustermap", gbs2.ServerName)
	}

}

// Test Ready ✅
func TestBackgroundJobForSuspectedNodeMoveToNoGossip(t *testing.T) {

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
	gbs2.gossip.gossipControlChannel <- false
	time.Sleep(15 * time.Second)

	gbs2.Shutdown()
	gbs.Shutdown()

	if _, exists := gbs.notToGossipNodeStore.Load(gbs2.ServerName); !exists {
		t.Errorf("node %s should have been moved to not-to-gossip store", gbs2.ServerName)
	}

}

// Test Ready ✅
func TestBackgroundJobTombstoneNode(t *testing.T) {

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
	gbs2.gossip.gossipControlChannel <- false
	time.Sleep(25 * time.Second)

	gbs2.Shutdown()
	gbs.Shutdown()

	if _, exists := gbs.notToGossipNodeStore.Load(gbs2.ServerName); !exists {
		t.Errorf("node %s should have been moved to not-to-gossip store", gbs2.ServerName)
	}

	if n, exists := gbs.clusterMap.participants[gbs2.ServerName]; !exists {
		t.Errorf("node %s should exist in our clustermap", gbs2.ServerName)
	} else {
		if len(n.keyValues) != 1 {
			t.Errorf("node %s should only have one k-v in their map - got length %d", gbs2.ServerName, len(n.keyValues))
		}
		if n.f.state != FAULTY {
			t.Errorf("node %s should have been FAULTY", gbs2.ServerName)
		}
	}

	if n, exists := gbs.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(FAILURE_DKG, gbs2.ServerName)]; !exists {
		t.Errorf("node %s should exist in our failure view", gbs2.ServerName)
	} else {
		if n.Value[0] != FAULTY {
			t.Errorf("node %s should be marked as fualty in our failure view", gbs2.ServerName)
		}
	}

}

func TestFaultyGossipedToOtherNode(t *testing.T) {

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

	nodeCfg3 := `
				Name = "t-3"
				Host = "localhost"
				Port = "8083"
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

	gbs3, err := NewServerFromConfigString(nodeCfg3, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(500 * time.Millisecond)
	go gbs2.StartServer()
	go gbs3.StartServer()

	time.Sleep(5 * time.Second)
	gbs2.Shutdown()

	time.Sleep(60 * time.Second)

	// gbs3 should not be gossiping with node 2 and both should mark gbs2 as faulty

	if _, exists := gbs.notToGossipNodeStore.Load(gbs2.ServerName); !exists {
		t.Errorf("node %s should have been moved to not-to-gossip store", gbs2.ServerName)
	}

	if n, exists := gbs.clusterMap.participants[gbs2.ServerName]; !exists {
		t.Errorf("node %s should exist in our clustermap", gbs2.ServerName)
	} else {
		if len(n.keyValues) != 1 {
			t.Errorf("node %s should only have one k-v in their map - got length %d", gbs2.ServerName, len(n.keyValues))
		}
		if n.f.state != FAULTY {
			t.Errorf("node %s should have been FAULTY", gbs2.ServerName)
		}
	}

	if n, exists := gbs3.clusterMap.participants[gbs3.ServerName].keyValues[MakeDeltaKey(FAILURE_DKG, gbs2.ServerName)]; !exists {
		t.Errorf("node %s should exist in our failure view", gbs2.ServerName)
	} else {
		if n.Value[0] != FAULTY {
			t.Errorf("node %s should be marked as fualty in our failure view", gbs2.ServerName)
		}
	}

	if _, exists := gbs3.notToGossipNodeStore.Load(gbs2.ServerName); !exists {
		t.Errorf("node %s should have been moved to not-to-gossip store", gbs2.ServerName)
	}

	if n, exists := gbs3.clusterMap.participants[gbs2.ServerName]; !exists {
		t.Errorf("node %s should exist in our clustermap", gbs2.ServerName)
	} else {
		if len(n.keyValues) != 1 {
			t.Errorf("node %s should only have one k-v in their map - got length %d", gbs2.ServerName, len(n.keyValues))
		}
		if n.f.state != FAULTY {
			t.Errorf("node %s should have been FAULTY", gbs2.ServerName)
		}
	}

	if n, exists := gbs3.clusterMap.participants[gbs3.ServerName].keyValues[MakeDeltaKey(FAILURE_DKG, gbs2.ServerName)]; !exists {
		t.Errorf("node %s should exist in our failure view", gbs2.ServerName)
	} else {
		if n.Value[0] != FAULTY {
			t.Errorf("node %s should be marked as fualty in our failure view", gbs2.ServerName)
		}
	}

}
