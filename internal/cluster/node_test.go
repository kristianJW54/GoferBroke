package cluster

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

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
				fmt.Printf("name: %s\n", name)
				if _, exists := tt.addrMapCheck[name]; !exists {
					t.Errorf("%s not found in addr map", name)
				}
				for _, addr := range value {
					if !slices.Contains(tt.addrMapCheck[name], addr) {
						t.Errorf("%s not found in addr map", name)
					}
					fmt.Println("addr:\n", addr)
				}
			}

		})
	}
}

func TestPercDiff(t *testing.T) {

	connCount := 3
	knownCount := 1

	res := percMakeup(knownCount, connCount)

	fmt.Println(res)

}

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

	t.Logf("Selected node ID: %s", conn.name)

	if conn.name == node1 {
		t.Errorf("selected node should not select it self")
	}

}

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

	node1Cfg := `
			Name = "node-1"
			Host = "localhost"
			Port = "8081"
			IsSeed = True`

	gbs, err := NewServerFromConfigString(node1Cfg, cfg)
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
		fmt.Printf("Seed address: %s\n", addr.resolved.String())

		if addr.resolved.String() == gbs.advertiseAddress.String() {
			t.Errorf("retrieved seed addr should NOT be our own")
		}

	} else {
		t.Errorf("our advertise addr is not the same as our seed addr in seed list - %s - %s", gbs.advertiseAddress.String(), gbs.seedAddr[1].resolved.String())
	}

}

func TestGetClusterConfigDeltas(t *testing.T) {

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

func TestGetClusterConfigUpdateExchange(t *testing.T) {

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True	
				}

`

	node2Cfg := `
				Name = "node-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
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

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}
	gbs2, err := NewServerFromConfigString(node2Cfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	configKey := "Name"

	go gbs.StartServer()
	time.Sleep(1 * time.Second)

	//p := gbs.clusterMap.participants[gbs.ServerName]

	// TODO This needs to be an update delta function with a potential callback or if block to also update struct if CONFIG_DKG
	//gbs.gbClusterConfig.Name = "new-cluster"
	//p.keyValues[configKey].Value = []byte("new-cluster")
	//p.keyValues[configKey].Version++

	err = gbs.updateSelfInfo(CONFIG_DKG, configKey, D_STRING_TYPE, "new-cluster")
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

	time.Sleep(1 * time.Second)

	if gbs2.gbClusterConfig.Name != "new-cluster" {
		t.Errorf("expected to get config name of [new-cluster] --> got %s", gbs2.gbClusterConfig.Name)
	}

	self := gbs2.GetSelfInfo()
	for k, v := range self.keyValues {
		fmt.Printf("k = %s, v = %v\n", k, v.Version)
	}
	fmt.Printf("gbs2 name = %s\n", gbs2.gbClusterConfig.Name)

	gbs2.Shutdown()
	gbs.Shutdown()

}

// Test Ready ✅
func TestSendProbe(t *testing.T) {

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
