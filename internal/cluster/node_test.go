package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Test Ready ✅
func TestCreateNodeClient(t *testing.T) {

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

	c = gbs.createNodeClient(client, "node-client", NODE)

	time.Sleep(1 * time.Second)

	if !c.flags.isSet(WRITE_LOOP_STARTED) || !c.flags.isSet(READ_LOOP_STARTED) {
		t.Errorf("expected both read and write loops to have started")
	}

	if c.name == "" {
		t.Errorf("client should have a name")
	}

	if _, ok := gbs.tmpConnStore.Load(c.cid); !ok {
		t.Errorf("client should be stored in tempStore")
	}

	time.Sleep(1 * time.Second)

}

// Test Ready ✅
func TestBuildAddrMap(t *testing.T) {

	gbs := GenerateDefaultTestServer("main-server", addressTestingKVs, 5)

	testStruct := []struct {
		name         string
		known        []string
		addrMapCheck map[string][]string
	}{
		{
			name: "normal - three participants with addresses",
			known: []string{
				gbs.clusterMap.participantArray[1],
				gbs.clusterMap.participantArray[2],
			},
			addrMapCheck: map[string][]string{
				gbs.clusterMap.participantArray[0]: {
					_ADDRESS_,
				},
				gbs.clusterMap.participantArray[3]: {
					_ADDRESS_,
				},
				gbs.clusterMap.participantArray[4]: {
					_ADDRESS_,
				},
			},
		},
		{
			name: "extra address in config",
			known: []string{
				gbs.clusterMap.participantArray[1],
				gbs.clusterMap.participantArray[3],
			},
			addrMapCheck: map[string][]string{
				gbs.clusterMap.participantArray[0]: {
					_ADDRESS_,
					"CLOUD",
				},
				gbs.clusterMap.participantArray[2]: {
					_ADDRESS_,
				},
				gbs.clusterMap.participantArray[4]: {
					_ADDRESS_,
				},
			},
		},
	}

	for _, tt := range testStruct {
		t.Run(tt.name, func(t *testing.T) {

			am, err := gbs.buildAddrGroupMap(tt.known)
			if err != nil {
				t.Error(err)
			}

			for name, value := range am {
				if _, exists := tt.addrMapCheck[name]; !exists {
					t.Errorf("%s not found in addr map", name)
				}
				for _, addr := range value {
					if !slices.Contains(tt.addrMapCheck[name], addr) {
						t.Errorf("%s not found in addr map", name)
					}
				}
			}

		})
	}
}

// Test Ready ✅
func TestRetrieveASeedConn(t *testing.T) {

	now := time.Now().Unix()

	node1 := fmt.Sprintf("node-1@%d", now)
	node2 := fmt.Sprintf("node-2@%d", now)
	node3 := fmt.Sprintf("node-3@%d", now)

	c1 := &gbClient{
		name: node1,
	}
	c2 := &gbClient{
		name: node2,
	}
	c3 := &gbClient{
		name: node3,
	}

	gbs := &GBServer{
		ServerName: "node-1",
		ServerID: ServerID{
			name:     "node-1",
			uuid:     "node-1",
			timeUnix: uint64(now),
		},
		isSeed: true,
		seedAddr: []*seedEntry{
			{nodeID: node1},
			{nodeID: node2},
			{nodeID: node3},
		},
		clusterMap: ClusterMap{
			participantArray: []string{node1, node2, node3},
		},
		nodeConnStore: sync.Map{},
	}

	gbs.nodeConnStore.Store(node1, c1)
	gbs.nodeConnStore.Store(node2, c2)
	gbs.nodeConnStore.Store(node3, c3)

	conn, err := gbs.retrieveASeedConn(true)
	if err != nil {
		t.Error(err)
	}

	if conn.name == node1 {
		t.Errorf("selected node should not select it self")
	}

}

// Test Ready ✅
func TestGetRandomSeedToDial(t *testing.T) {

	cfg := `
			Name = "test-cluster"
			SeedServers = [
				{Host: "localhost", Port: "8080"},
				{Host: "localhost", Port: "8081"},
				{Host: "localhost", Port: "8082"},
			]
			Cluster {
				ClusterNetworkType = "LOCAL"
			}`

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

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Error(err)
	}

	// First need to check that advertise addr is the same as our seedAddr resolved
	if gbs.advertiseAddress.String() == gbs.seedAddr[1].resolved.String() {
		// Now we can try to get a random seed to dial
		addr, err := gbs.getRandomSeedToDial()
		if err != nil {
			t.Error(err)
		}

		if addr.resolved.String() == gbs.advertiseAddress.String() {
			t.Errorf("retrieved seed addr should NOT be our own")
		}

	} else {
		t.Errorf("our advertise addr is not the same as our seed addr in seed list - %s - %s", gbs.advertiseAddress.String(), gbs.seedAddr[1].resolved.String())
	}

}

// Test Ready ✅
func TestGetClusterConfigDeltas(t *testing.T) {

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
		t.Errorf("%v", err)
	}

	configKey := "config:Name"

	go gbs.StartServer()

	time.Sleep(1 * time.Second)

	p := gbs.clusterMap.participants[gbs.ServerName]

	// TODO This needs to be an update delta function with a potential callback or if block to also update struct if CONFIG_DKG
	gbs.gbClusterConfig.Name = "new-cluster"
	p.keyValues[configKey].Value = []byte("new-cluster")
	p.keyValues[configKey].Version++

	d, _, err := gbs.getConfigDeltasAboveVersion(0)
	if err != nil {
		t.Errorf("%v", err)
	}

	if len(d[gbs.ServerName]) != 1 {
		t.Errorf("expected to find a single delta for config key: %s --> got %d", configKey, len(d[configKey]))
	}

	gbs.Shutdown()

}

// Test Ready ✅
func TestGetClusterConfigUpdateFromChecksum(t *testing.T) {

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

	configKey := "Name"

	newClusterName := "new-cluster"

	go gbs.StartServer()
	time.Sleep(1 * time.Second)

	v, err := encodeValue(D_STRING_TYPE, newClusterName)
	if err != nil {
		t.Errorf("encoding value error %v", err)
	}
	err = gbs.updateSelfInfo(&Delta{KeyGroup: CONFIG_DKG, Key: configKey, Version: time.Now().Unix(), ValueType: D_STRING_TYPE, Value: v})
	if err != nil {
		t.Errorf("%v", err)
	}

	d, _, err := gbs.getConfigDeltasAboveVersion(0)
	if err != nil {
		t.Errorf("%v", err)
	}

	if len(d[gbs.ServerName]) != 1 {
		t.Errorf("expected to find a single delta for config key: %s --> got %d", configKey, len(d[configKey]))
	}

	go gbs2.StartServer()

	time.Sleep(2 * time.Second)

	if gbs2.gbClusterConfig.Name != "new-cluster" {
		t.Errorf("expected to get config name of [new-cluster] --> got %s", gbs2.gbClusterConfig.Name)
	}

	gbs2.Shutdown()
	gbs.Shutdown()

	gbs2NewClusterName := gbs2.gbClusterConfig.Name
	gbs2NewClusterNameDelta := gbs2.clusterMap.participants[gbs2.ServerName].keyValues[MakeDeltaKey(CONFIG_DKG, configKey)].Value
	gbs2NewClusterNameDeltaForGbs := gbs2.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(CONFIG_DKG, configKey)].Value

	if gbs2NewClusterName != newClusterName {
		t.Errorf("expected to get config name of %s --> got %s", newClusterName, gbs2NewClusterName)
	}

	if string(gbs2NewClusterNameDelta) != string(gbs2NewClusterNameDeltaForGbs) && string(gbs2NewClusterNameDelta) != newClusterName {
		t.Errorf("expected to get delta of %s --> got %s", newClusterName, gbs2NewClusterNameDelta)
	}

}

// Test Ready ✅
func TestUpdateClusterConfigFromNewDelta(t *testing.T) {

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

	configKey := "Name"

	newClusterName := "new-cluster"

	go gbs.StartServer()
	time.Sleep(100 * time.Millisecond)
	go gbs2.StartServer()

	time.Sleep(1 * time.Second)

	v, err := encodeValue(D_STRING_TYPE, newClusterName)
	if err != nil {
		t.Errorf("encoding value error %v", err)
	}
	err = gbs.updateSelfInfo(&Delta{KeyGroup: CONFIG_DKG, Key: configKey, Version: time.Now().Unix(), ValueType: D_STRING_TYPE, Value: v})
	if err != nil {
		t.Errorf("%v", err)
	}
	if err != nil {
		t.Errorf("%v", err)
	}

	d, _, err := gbs.getConfigDeltasAboveVersion(0)
	if err != nil {
		t.Errorf("%v", err)
	}

	if len(d[gbs.ServerName]) != 1 {
		t.Errorf("expected to find a single delta for config key: %s --> got %d", configKey, len(d[configKey]))
	}

	time.Sleep(4 * time.Second)

	if gbs2.gbClusterConfig.Name != "new-cluster" {
		t.Errorf("expected to get config name of [new-cluster] --> got %s", gbs2.gbClusterConfig.Name)
	}

	gbs2.Shutdown()
	gbs.Shutdown()

	gbs2NewClusterName := gbs2.gbClusterConfig.Name
	gbs2NewClusterNameDelta := gbs2.clusterMap.participants[gbs2.ServerName].keyValues[MakeDeltaKey(CONFIG_DKG, configKey)].Value
	gbs2NewClusterNameDeltaForGbs := gbs2.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(CONFIG_DKG, configKey)].Value

	if gbs2NewClusterName != newClusterName {
		t.Errorf("expected to get config name of %s --> got %s", newClusterName, gbs2NewClusterName)
	}

	if string(gbs2NewClusterNameDelta) != string(gbs2NewClusterNameDeltaForGbs) && string(gbs2NewClusterNameDelta) != newClusterName {
		t.Errorf("expected to get delta of %s --> got %s", newClusterName, gbs2NewClusterNameDelta)
	}

}

// Test Ready ✅
func TestConnectToSeedAndSeedSendSelf(t *testing.T) {

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
	time.Sleep(100 * time.Millisecond)
	go gbs2.StartServer()

	time.Sleep(2 * time.Second)

	if _, exists, err := gbs2.getNodeConnFromStore(gbs.ServerName); !exists {
		t.Errorf("%s should exist in %s node conn store", gbs.ServerName, gbs2.PrettyName())
	} else {
		if err != nil {
			t.Errorf("got error getting conn from store: %v", err)
		}
	}

	gbs.Shutdown()
	gbs2.Shutdown()

	var want struct {
		Msg string
	}

	for _, snap := range gbs2.jsonBuffer.Snapshot() {
		var rec struct {
			Time  string `json:"time"`
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}

		if err := json.Unmarshal(snap, &rec); err != nil {
			t.Fatalf("bad log json: %v\n%s", err, string(snap))
		}
		if rec.Level == "INFO" {
			// Test Assert here
			if strings.Contains(rec.Msg, "Connected to seed") {
				want = struct{ Msg string }{Msg: rec.Msg}
				break
			}
			continue
		}
	}

	if want.Msg == "" {
		t.Errorf("expected to get message -> %s", "Connected to seed")
	}

}

// Connect to node in map
