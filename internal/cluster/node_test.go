package cluster

import (
	"fmt"
	"log"
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
				log.Printf("name: %s", name)
				if _, exists := tt.addrMapCheck[name]; !exists {
					t.Errorf("%s not found in addr map", name)
				}
				for _, addr := range value {
					if !slices.Contains(tt.addrMapCheck[name], addr) {
						t.Errorf("%s not found in addr map", name)
					}
					log.Println("addr:", addr)
				}
			}

		})
	}
}

func TestPercDiff(t *testing.T) {

	connCount := 3
	knownCount := 1

	res := percMakeup(knownCount, connCount)

	log.Println(res)

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
		log.Printf("Seed address: %s", addr.resolved.String())

		if addr.resolved.String() == gbs.advertiseAddress.String() {
			t.Errorf("retrieved seed addr should NOT be our own")
		}

	} else {
		t.Errorf("our advertise addr is not the same as our seed addr in seed list - %s - %s", gbs.advertiseAddress.String(), gbs.seedAddr[1].resolved.String())
	}

}
