package cluster

import (
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"
)

func TestSystemUtil(t *testing.T) {

	getMemory()

	fmt.Println(runtime.NumCPU())

}

func TestGetFields(t *testing.T) {

	sm, smMap := newSystemMetrics()

	if sm == nil {
		t.Errorf("system metrics is nil")
	}

	if len(smMap) == 0 {
		t.Errorf("system metrics map is empty")
	}

}

func TestBackGroundMetricChecks(t *testing.T) {

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

	// Change a memory field and delta so background process can correct it
	gbs.sm.UsedPercent = 0.1
	perc := make([]byte, 8)
	binary.BigEndian.PutUint64(perc, math.Float64bits(0.1))
	gbs.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(SYSTEM_DKG, _MEM_PERC_)].Value = perc

	time.Sleep(10 * time.Second)

	if gbs.sm.UsedPercent == 0.1 {
		t.Errorf("used percent should have changed")
	}

}
