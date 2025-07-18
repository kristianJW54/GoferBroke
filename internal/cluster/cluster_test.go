package cluster

import (
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func makeTestDeltas(t *testing.T, numberOfDeltas int, versionBaseline int64, numberOfOutdated int, overLoadBaseline, numberOfOverloaded int) map[string]*Delta {

	t.Helper()

	d := make(map[string]*Delta, numberOfDeltas)
	keyGroup := "TEST"
	key := "key"

	outdatedLeft := numberOfOutdated
	overloadedLeft := numberOfOverloaded

	for i := 0; i < numberOfDeltas; i++ {

		var version int64
		if outdatedLeft > 0 {
			version = (versionBaseline - int64(numberOfDeltas)) - 1
			outdatedLeft--
		} else {
			version = versionBaseline - int64(i) // ascending unique versions
		}

		var buff []byte
		if overloadedLeft > 0 {
			buff = make([]byte, overLoadBaseline+10) // deliberately over sized
			overloadedLeft--
		} else {
			buff = []byte{} // empty
		}

		currKey := fmt.Sprintf("%s%d", key, i)

		d[MakeDeltaKey(keyGroup, currKey)] = &Delta{
			KeyGroup:  keyGroup,
			Key:       currKey,
			Version:   version,
			ValueType: D_BYTE_TYPE,
			Value:     buff,
		}

	}

	return d

}

// Test discovery by test config and turning off gossip

func TestParticipantHeapDepthFirst(t *testing.T) {

	heapM1 := makeTestDeltas(t, 4, 1640995213, 0, 0, 0)
	heapM2 := makeTestDeltas(t, 15, 1640995213, 0, 0, 0)

	// Generate test server

	gbs := &GBServer{
		ServerName: "test-server-1",
		clusterMap: ClusterMap{
			participants:     make(map[string]*Participant, 3),
			participantArray: make([]string, 3),
		},
	}

	gbs.clusterMap.participants[gbs.ServerName] = &Participant{
		name:       gbs.ServerName,
		keyValues:  heapM2,
		maxVersion: 1640995213,
	}
	gbs.clusterMap.participantArray[0] = gbs.ServerName

	// Now need to create a fake second node

	gbs.clusterMap.participants["second-node"] = &Participant{
		name:       "second-node",
		keyValues:  heapM1,
		maxVersion: 1640995213,
	}
	gbs.clusterMap.participantArray[1] = "second-node"

	// Now need to create a fake third node

	gbs.clusterMap.participants["third-node"] = &Participant{
		name:       "third-node",
		keyValues:  heapM2,
		maxVersion: 1640995213,
	}
	gbs.clusterMap.participantArray[2] = "third-node"

	// Generate a fake digest received from second node

	d := &fullDigest{

		"second-node": &digest{
			"second-node",
			0,
		},
		"third-node": &digest{
			"third-node",
			1640995208,
		},
		gbs.name: &digest{
			gbs.ServerName,
			0,
		},
	}

	// Main node receives a digest with 3 nodes including itself - ALL are outdated. Meaning it will have to prioritise
	// Participants with the most available deltas it has in its map in this case -> third-node

	ph, err := gbs.generateParticipantHeap(gbs.ServerName, d)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(ph[0].name)

	if ph[0].name != "third-node" {
		t.Fatalf("participant heap should have prioritised third-node - got %s instead", ph[0].name)
	}

}

func TestBuildDeltaOutdatedOnly(t *testing.T) {

	// We are testing that only outdated deltas are including in the build delta list

	version := int64(1640995213)
	nod := 4

	normalKV := makeTestDeltas(t, nod, version, 0, 0, 0)

	gbs := &GBServer{
		ServerName: "test-server-1",
		clusterMap: ClusterMap{
			participants:     make(map[string]*Participant, 2),
			participantArray: make([]string, 2),
		},
		gbClusterConfig: &GbClusterConfig{
			Name: "test-cluster",
			Cluster: &ClusterOptions{
				MaxDeltaSize: 256,
			},
		},
	}

	gbs.clusterMap.participants[gbs.ServerName] = &Participant{
		name:       gbs.ServerName,
		keyValues:  normalKV,
		maxVersion: version,
	}
	gbs.clusterMap.participantArray[0] = gbs.ServerName

	// Now make fake second node

	gbs.clusterMap.participants["second-node"] = &Participant{
		name:       "second-node",
		keyValues:  normalKV,
		maxVersion: version,
	}
	gbs.clusterMap.participantArray[1] = "second-node"

	// Main node will have 3 newer deltas than the other node
	// Both will have 5 deltas
	// Delta list should only have 3 deltas

	// We build a fake digest to say that the second node has an outdated map of the main node

	d := &fullDigest{

		"second-node": &digest{
			"second-node",
			version,
		},
		gbs.ServerName: &digest{
			gbs.ServerName,
			version - 2,
		},
	}

	ph, err := gbs.generateParticipantHeap("second-node", d)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("ph = %s - max version %v - peerMaxVersion %v", ph[0].name, ph[0].maxVersion, ph[0].peerMaxVersion)

	dl, _, err := gbs.buildDelta(&ph, 1000)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("length of dl - %v", len(dl[gbs.ServerName]))

	if len(dl[gbs.ServerName]) != 2 {
		t.Errorf("expected to get deltas list of 2 but got %v", len(dl[gbs.ServerName]))
	}

}

// TODO Finish fulling testing build delta for all cases

func TestDeltaHeap(t *testing.T) {

	// Create keyValues with PBDelta messages
	var keyValues = map[string]*Delta{
		"TEST:key6":  {KeyGroup: "TEST", Key: "key6", ValueType: INTERNAL_D, Version: 1640995203, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"TEST:key7":  {KeyGroup: "TEST", Key: "key7", ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("A")},
		"TEST:key8":  {KeyGroup: "TEST", Key: "key8", ValueType: INTERNAL_D, Version: 1640995206, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"TEST:key9":  {KeyGroup: "TEST", Key: "key9", ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("😃 Emoji support test.")},
		"TEST:key10": {KeyGroup: "TEST", Key: "key10", ValueType: INTERNAL_D, Version: 1640995208, Value: []byte("Another simple string.")},
	}

	dh := make(deltaHeap, 5)

	i := 0
	for _, value := range keyValues {

		dh[i] = &deltaQueue{
			key:      value.Key,
			overload: false,
			version:  value.Version,
			index:    i,
		}
		i++
	}

	heap.Init(&dh)

	assertion := [5]int{1640995203, 1640995205, 1640995206, 1640995207, 1640995208}

	for i := 0; i < len(assertion); i++ {

		result := heap.Pop(&dh).(*deltaQueue).version

		if assertion[i] != int(result) {
			t.Errorf("Expected %d, got %d", assertion[i], result)
			return
		} else {
			log.Printf("Version %d --> assertion %d", assertion[i], result)
		}
	}
}

func TestUpdateHeartBeat(t *testing.T) {

	keyValues := map[string]*Delta{
		"system:heartbeat": &Delta{KeyGroup: SYSTEM_DKG, ValueType: INTERNAL_D, Key: _HEARTBEAT_, Version: 1640995200, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		"system:tcp":       &Delta{KeyGroup: SYSTEM_DKG, ValueType: INTERNAL_D, Key: _ADDRESS_, Version: 1640995200, Value: []byte("127.0.0.0.1:8081")},
	}

	// Initialize config with the seed server address
	gbs := GenerateDefaultTestServer("test-1", keyValues, 0)

	self := gbs.GetSelfInfo()

	log.Printf("heartbeat - %d", self.keyValues[MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_)].Version)

	err := gbs.updateHeartBeat(time.Now().Unix())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("new heartbeat - %d", self.keyValues[MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_)].Version)

}

// Good test - keep
func TestClusterMapLocks(t *testing.T) {

	// To test cluster map locks under high concurrency by having worker routines reading cluster map,
	// writing new participants and writing current participant value and updating self

	// Need tasks and workers to assign the tasks to
	// Tasks should be: Add participant, read cluster map, update cluster map, update self info

	// Initialize config with the seed server address
	mockServer := GenerateDefaultTestServer("mock-server", heartBeatKV, 1)

	// Concurrency settings
	numWorkers := 10
	tasks := 100
	var wg sync.WaitGroup
	taskQ := make(chan func(), tasks)

	worker := func(id int) {
		defer wg.Done()
		for task := range taskQ {
			task()
		}
	}

	// Define the tasks
	readClusterMap := func() {
		mockServer.clusterMapLock.RLock()
		defer mockServer.clusterMapLock.RUnlock()
	}

	updateSelfInfo := func() {
		err := mockServer.updateSelfInfo(SYSTEM_DKG, _HEARTBEAT_, D_INT64_TYPE, time.Now().Unix())
		if err != nil {
			t.Errorf("%v", err)
		}
	}

	addParticipant := func() {
		tmp := &tmpParticipant{
			keyValues: keyValues1,
		}
		err := mockServer.addParticipantFromTmp("participant", tmp)
		if err != nil {
			t.Errorf("add participant failed: %v", err)
		}
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i)
	}

	for i := 0; i < tasks; i++ {
		switch i % 3 {
		case 0:
			taskQ <- addParticipant
		case 1:
			taskQ <- updateSelfInfo
		case 2:
			taskQ <- readClusterMap
		}
	}

	close(taskQ)

	wg.Wait()
	log.Println("Tasks complete")

}

// Deep copy helper
func cloneKeyValues(src map[string]*Delta) map[string]*Delta {
	dst := make(map[string]*Delta, len(src))
	for k, v := range src {
		copys := *v
		copys.Value = append([]byte(nil), v.Value...) // clone []byte
		dst[k] = &copys
	}
	return dst
}

// Good test - keep
func TestGSATwoNodes(t *testing.T) {

	now := time.Now().Unix()

	newer := map[string]*Delta{
		"test:key1": &Delta{KeyGroup: TEST_DKG, Key: "key1", ValueType: D_STRING_TYPE, Version: now, Value: []byte("I am a delta blissfully unaware as to how annoying I am to code")},
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: D_STRING_TYPE, Version: now, Value: []byte("Try to gossip about me and see what happens")},
	}

	outdated := map[string]*Delta{
		"test:key1": &Delta{KeyGroup: TEST_DKG, Key: "key1", ValueType: D_STRING_TYPE, Version: now - 2, Value: []byte("I am a delta blissfully")},
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: D_STRING_TYPE, Version: now - 1, Value: []byte("I Don't Like Gossipers")},
	}

	brandNew := map[string]*Delta{
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: D_STRING_TYPE, Version: now - 4, Value: []byte("One delta andy over here")},
	}

	tests := []struct {
		name             string
		node1            *GBServer
		node2            *GBServer
		shouldHaveDelta  bool
		participantCount int
		digestCount      int
	}{
		{
			name: "baseline test - two nodes with node-2 having outdated values of node-1 = node-1 should gossip newer",
			node1: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-1", newer, 1)

				gbs.clusterMap.participants["node-2"] = &Participant{
					name:       "node-2",
					keyValues:  outdated,
					maxVersion: outdated["test:key2"].Version - 2,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

				return gbs
			}(),
			node2: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-2", outdated, 1)

				gbs.clusterMap.participants["node-1"] = &Participant{
					name:       "node-1",
					keyValues:  cloneKeyValues(outdated), // ✅ deep copy avoids shared pointer problem
					maxVersion: outdated["test:key2"].Version - 2,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-1")

				return gbs

			}(),
			shouldHaveDelta:  false,
			participantCount: 2,
			digestCount:      2,
		},
		{
			name: "node-1 has an extra participant that node-2 doesn't know about",
			node1: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-1", newer, 1)
				gbs.clusterMap.participants["node-2"] = &Participant{
					name:       "node-2",
					keyValues:  outdated,
					maxVersion: outdated["test:key2"].Version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

				gbs.clusterMap.participants["node-3"] = &Participant{
					name:       "node-3",
					keyValues:  brandNew,
					maxVersion: brandNew["test:key2"].Version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-3")

				return gbs

			}(),
			node2: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-2", outdated, 1)

				gbs.clusterMap.participants["node-1"] = &Participant{
					name:       "node-1",
					keyValues:  cloneKeyValues(outdated),
					maxVersion: outdated["test:key2"].Version - 2,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-1")

				return gbs

			}(),
			shouldHaveDelta:  true,
			participantCount: 3,
			digestCount:      3,
		},
		{
			name: "node-2 will have a lesser max version to node-1",
			node1: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-1", newer, 1)

				gbs.clusterMap.participants["node-2"] = &Participant{
					name:       "node-2",
					keyValues:  outdated,
					maxVersion: outdated["test:key2"].Version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

				return gbs

			}(),
			node2: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-2", outdated, 1)
				gbs.clusterMap.participants[gbs.ServerName].maxVersion = now - 2

				gbs.clusterMap.participants["node-1"] = &Participant{
					name:       "node-1",
					keyValues:  newer,
					maxVersion: newer["test:key2"].Version - 2,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-1")

				return gbs

			}(),
			shouldHaveDelta:  false,
			participantCount: 2,
			digestCount:      2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Generate digest from lower version node
			d, _, err := tt.node2.generateDigest()
			if err != nil {
				t.Errorf("generate digest failed: %v", err)
			}

			name, fd, err := deSerialiseDigest(d)
			if err != nil {
				t.Errorf("deSerialise digest failed: %v", err)
			}

			// Prepare GSA
			gsa, err := tt.node1.prepareGossSynAck(name, fd)
			if err != nil {
				t.Errorf("prepareGossSynAck failed: %v", err)
			}

			_, newFd, newCd, err := deserialiseGSA(gsa)
			if err != nil {
				t.Errorf("deserialise GSA failed: %v", err)
			}

			if newCd != nil {

				err = tt.node2.addGSADeltaToMap(newCd)
				if err != nil {
					t.Errorf("addGSADeltaToMap failed: %v", err)
				}

				// Delta assertions
				for name, parts := range tt.node2.clusterMap.participants {
					if name != tt.node1.clusterMap.participants[name].name {
						t.Errorf("participant name does not match - got %s - want %s", name, tt.node1.clusterMap.participants[name].name)
					}

					for k, v := range parts.keyValues {
						if !bytes.Equal(v.Value, tt.node1.clusterMap.participants[name].keyValues[k].Value) {
							t.Errorf("value does not match - got %s - want %s", v.Value, tt.node1.clusterMap.participants[name].keyValues[k].Value)
						}
					}

				}

			}

			// Test assertions

			if len(*newFd) != tt.digestCount {
				t.Errorf("different digest count = %d, want %d", len(*newFd), tt.digestCount)
			}

			if newCd == nil && tt.shouldHaveDelta {
				t.Errorf("should have delta")
			}

			if newCd != nil {
				if len(tt.node2.clusterMap.participantArray) != tt.participantCount {
					t.Errorf("should have %d participants, got %d", tt.participantCount, len(tt.node2.clusterMap.participantArray))
				}
			}

		})
	}
}

// Test is good to keep
func TestAddGSADeltaToMap(t *testing.T) {

	now := time.Now().Unix()

	node1KeyValues := map[string]*Delta{
		"test:key1": &Delta{KeyGroup: TEST_DKG, Key: "key1", ValueType: D_STRING_TYPE, Version: now, Value: []byte("I am a delta blissfully unaware as to how annoying I am to code")},
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: D_STRING_TYPE, Version: now, Value: []byte("Try to gossip about me and see what happens")},
	}

	node2KeyValues := map[string]*Delta{
		"test:key1": &Delta{KeyGroup: TEST_DKG, Key: "key1", ValueType: D_STRING_TYPE, Version: now, Value: []byte("I am a delta blissfully")},
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: D_STRING_TYPE, Version: now, Value: []byte("I Don't Like Gossipers")},
	}

	node3KeyValues := map[string]*Delta{
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: D_STRING_TYPE, Version: now - 2, Value: []byte("One delta andy over here - Im the newer version boi ;)")},
	}

	node3KeyValuesOutdated := map[string]*Delta{
		"test:key2": &Delta{KeyGroup: TEST_DKG, Key: "key2", ValueType: D_STRING_TYPE, Version: now - 4, Value: []byte("One delta andy over here")},
	}

	// Node 1

	gbs := GenerateDefaultTestServer("node-1", node1KeyValues, 0)

	gbs.clusterMap.participants["node-2"] = &Participant{
		name:       "node-2",
		keyValues:  node2KeyValues,
		maxVersion: node2KeyValues["test:key2"].Version,
	}

	gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

	gbs.clusterMap.participants["node-3"] = &Participant{
		name:       "node-3",
		keyValues:  node3KeyValues,
		maxVersion: node3KeyValues["test:key2"].Version,
	}

	gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-3")

	// Node 2

	gbs2 := GenerateDefaultTestServer("node-2", node2KeyValues, 0)
	gbs2.clusterMap.participants[gbs2.ServerName].maxVersion = now - 2

	gbs2.clusterMap.participants["node-1"] = &Participant{
		name:       "node-1",
		keyValues:  node1KeyValues,
		maxVersion: node1KeyValues["test:key2"].Version,
	}

	gbs2.clusterMap.participantArray = append(gbs2.clusterMap.participantArray, "node-1")

	gbs2.clusterMap.participants["node-3"] = &Participant{
		name:       "node-3",
		keyValues:  node3KeyValuesOutdated,
		maxVersion: node3KeyValuesOutdated["test:key2"].Version,
	}

	gbs2.clusterMap.participantArray = append(gbs2.clusterMap.participantArray, "node-3")

	// Generate digest from lower version node
	d, _, err := gbs2.generateDigest()
	if err != nil {
		t.Errorf("generate digest failed: %v", err)
	}

	name, fd, err := deSerialiseDigest(d)
	if err != nil {
		t.Errorf("deSerialise digest failed: %v", err)
	}

	// Prepare GSA
	gsa, err := gbs.prepareGossSynAck(name, fd)
	if err != nil {
		t.Errorf("prepareGossSynAck failed: %v", err)
	}

	_, _, newCd, err := deserialiseGSA(gsa)
	if err != nil {
		t.Errorf("deserialise GSA failed: %v", err)
	}

	// Add here
	err = gbs2.addGSADeltaToMap(newCd)
	if err != nil {
		t.Errorf("addGSADeltaToMap failed: %v", err)
	}

	keyCheck := gbs2.clusterMap.participants["node-3"].keyValues["test:key2"]

	if !bytes.Equal(keyCheck.Value, node3KeyValues["test:key2"].Value) {
		t.Errorf("gbs2 did not recieve the updated delta values from gbs - got %v, expected %v", keyCheck.Value, node3KeyValues["test:key2"].Value)
	}

	//log.Printf("keyCheck: version %v - value %s", keyCheck.Version, keyCheck.Value)

}

// Delta Handling Tests

func TestAddAndUpdateDelta(t *testing.T) {

	now := time.Now().Unix()

	gbs := GenerateDefaultTestServer("test-server", keyValues1, 1)

	deltaToADD := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       "test",
		ValueType: D_STRING_TYPE,
		Value:     []byte("test string being added after"),
		Version:   now,
	}

	self := gbs.GetSelfInfo()

	// Add
	err := self.Store(deltaToADD)
	if err != nil {
		t.Errorf("store delta failed: %v", err)
	}

	key := MakeDeltaKey(SYSTEM_DKG, deltaToADD.Key)

	if _, exists := self.keyValues[key]; !exists {
		t.Errorf("delta does not exist")
	}

	// Update

	updatedValue := []byte("test string being added after - this has been updated")

	deltaToUpdate := &Delta{
		KeyGroup:  SYSTEM_DKG,
		Key:       "test",
		ValueType: D_STRING_TYPE,
		Value:     updatedValue,
		Version:   now,
	}

	err = self.Update(SYSTEM_DKG, deltaToADD.Key, deltaToUpdate, func(toBeUpdated, by *Delta) {
		*toBeUpdated = *by
	})
	if err != nil {
		t.Errorf("update delta failed: %v", err)
	}

	log.Printf("delta = %s", self.keyValues[key].Value)

	if delta, exists := self.keyValues[key]; !exists {
		t.Errorf("delta does not exist")
	} else {
		if bytes.Compare(delta.Value, updatedValue) != 0 {
			t.Errorf("delta value not updated")
		}
	}

}
