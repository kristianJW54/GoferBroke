package cluster

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

// Test discovery by test config and turning off gossip

func TestParticipantHeap(t *testing.T) {

	gbs := GenerateDefaultTestServer("main-server", keyValues1, 5)

	participant := gbs.clusterMap.participants[gbs.name]

	ph := make(participantHeap, 0, 5)

	heap.Push(&ph, &participantQueue{
		name:            gbs.name,
		availableDeltas: 0,
		maxVersion:      participant.maxVersion,
	})

	for i := 1; i <= 4; i++ {

		partName := fmt.Sprintf("node%d", i)

		p := &Participant{
			name:       partName,
			keyValues:  participant.keyValues,
			maxVersion: participant.maxVersion,
		}

		mv := participant.maxVersion

		gbs.clusterMap.participants[partName] = p

		heap.Push(&ph, &participantQueue{
			name:            partName,
			availableDeltas: 0,
			maxVersion:      mv,
		})

	}

	heap.Init(&ph)

	assertion := participant.maxVersion

	versionCheck := heap.Pop(&ph).(*participantQueue).maxVersion

	log.Printf("Version check: %v --> assertion: %v", versionCheck, assertion)

	if versionCheck != assertion {
		t.Errorf("Version check failed. Expected %d, got %d", assertion, versionCheck)
	}

}

func TestBuildDelta(t *testing.T) {

	// Gen 3 test servers
	// Main has newest deltas
	// Other two have old
	// Gen participant heap
	// Build delta

	var keyValues1 = map[string]*Delta{
		"TEST:key6": {KeyGroup: "TEST", Key: "key6", ValueType: INTERNAL_D, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"TEST:key7": {KeyGroup: "TEST", Key: "key7", ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("A")},
		"TEST:key8": {KeyGroup: "TEST", Key: "key8", ValueType: INTERNAL_D, Version: 1640995206, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"TEST:key9": {KeyGroup: "TEST", Key: "key9", ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("😃 Emoji support test.")},
		//"TEST:key10": {keyGroup: "TEST", key: "key10", valueType: INTERNAL_D, version: 1640995208, value: []byte("Another simple string.")},
		//"TEST:key11": {keyGroup: "TEST", key: "key11", valueType: INTERNAL_D, version: 1640995209, value: []byte("This is test entry number eleven.")},
		//"TEST:key12": {keyGroup: "TEST", key: "key12", valueType: INTERNAL_D, version: 1640995210, value: []byte("Entry twelve includes a longer message to check proper handling.")},
		//"TEST:key13": {keyGroup: "TEST", key: "key13", valueType: INTERNAL_D, version: 1640995211, value: []byte("Testing entry for key 13, which is designed to be unique.")},
		//"TEST:key14": {keyGroup: "TEST", key: "key14", valueType: INTERNAL_D, version: 1640995212, value: []byte("Another example message for the fourteenth key.")},
		//"TEST:key15": {keyGroup: "TEST", key: "key15", valueType: INTERNAL_D, version: 1640995213, value: []byte("The fifteenth key's entry demonstrates variety in test data.")},
		//"TEST:key16": {keyGroup: "TEST", key: "key16", valueType: INTERNAL_D, version: 1640995214, value: []byte("Testing with key16. Another sample text here.")},
		//"TEST:key17": {keyGroup: "TEST", key: "key17", valueType: INTERNAL_D, version: 1640995215, value: []byte("Key 17: A different message string to showcase the test entry.")},
		//"TEST:key18": {keyGroup: "TEST", key: "key18", valueType: INTERNAL_D, version: 1640995216, value: []byte("Eighteenth key value: a blend of letters and numbers: 1234567890.")},
		//"TEST:key19": {keyGroup: "TEST", key: "key19", valueType: INTERNAL_D, version: 1640995217, value: []byte("Entry number 19 uses special characters: !@#$%^&*()")},
		//"TEST:key20": {keyGroup: "TEST", key: "key20", valueType: INTERNAL_D, version: 1640995218, value: []byte("The final test key entry, providing closure for our sample.")},
	}

	var keyValues1Outdated = map[string]*Delta{
		"TEST:key6":  {KeyGroup: "TEST", Key: "key6", ValueType: INTERNAL_D, Version: 1640995203, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"TEST:key7":  {KeyGroup: "TEST", Key: "key7", ValueType: INTERNAL_D, Version: 1640995204, Value: []byte("A")},
		"TEST:key8":  {KeyGroup: "TEST", Key: "key8", ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"TEST:key9":  {KeyGroup: "TEST", Key: "key9", ValueType: INTERNAL_D, Version: 1640995206, Value: []byte("😃 Emoji support test.")},
		"TEST:key10": {KeyGroup: "TEST", Key: "key10", ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("Another simple string.")},
		"TEST:key11": {KeyGroup: "TEST", Key: "key11", ValueType: INTERNAL_D, Version: 1640995208, Value: []byte("This is test entry number eleven.")},
		"TEST:key12": {KeyGroup: "TEST", Key: "key12", ValueType: INTERNAL_D, Version: 1640995209, Value: []byte("Entry twelve includes a longer message to check proper handling.")},
		"TEST:key13": {KeyGroup: "TEST", Key: "key13", ValueType: INTERNAL_D, Version: 1640995210, Value: []byte("Testing entry for key 13, which is designed to be unique.")},
		"TEST:key14": {KeyGroup: "TEST", Key: "key14", ValueType: INTERNAL_D, Version: 1640995211, Value: []byte("Another example message for the fourteenth key.")},
		"TEST:key15": {KeyGroup: "TEST", Key: "key15", ValueType: INTERNAL_D, Version: 1640995212, Value: []byte("The fifteenth key's entry demonstrates variety in test data.")},
		"TEST:key16": {KeyGroup: "TEST", Key: "key16", ValueType: INTERNAL_D, Version: 1640995213, Value: []byte("Testing with key16. Another sample text here.")},
		"TEST:key17": {KeyGroup: "TEST", Key: "key17", ValueType: INTERNAL_D, Version: 1640995214, Value: []byte("Key 17: A different message string to showcase the test entry.")},
		"TEST:key18": {KeyGroup: "TEST", Key: "key18", ValueType: INTERNAL_D, Version: 1640995215, Value: []byte("Eighteenth key value: a blend of letters and numbers: 1234567890.")},
		"TEST:key19": {KeyGroup: "TEST", Key: "key19", ValueType: INTERNAL_D, Version: 1640995216, Value: []byte("Entry number 19 uses special characters: !@#$%^&*()")},
		"TEST:key20": {KeyGroup: "TEST", Key: "key20", ValueType: INTERNAL_D, Version: 1640995217, Value: []byte("The final test key entry, providing closure for our sample.")},
	}

	gbs := GenerateDefaultTestServer("server-1", keyValues1, 10)
	gbs2 := GenerateDefaultTestServer("server-2", keyValues1Outdated, 2)

	d, size, err := gbs2.generateDigest()
	if err != nil {
		t.Errorf("Error generating digest: %v", err)
	}

	remaining := int(DEFAULT_MAX_GSA) - size

	_, fd, err := deSerialiseDigest(d)
	if err != nil {
		t.Errorf("Error de-serialising digest: %v", err)
	}

	ph, err := gbs.generateParticipantHeap(gbs2.ServerName, fd)

	delta, size, err := gbs.buildDelta(&ph, remaining)
	if err != nil {
		t.Errorf("Error building delta: %v", err)
	}

	if size > int(DEFAULT_MAX_GSA) {
		t.Errorf("size is greater than max delta size - got %v, expected to be less then --> %v", size, DEFAULT_MAX_GSA)
	}

	if len(delta) == 0 {
		t.Errorf("expected delta to have length greater than 0 - got %v", len(delta))
	}

}

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

	// Initialize config with the seed server address
	config := &GbClusterConfig{
		SeedServers: []*Seeds{
			{},
		},
		Cluster: &ClusterOptions{},
	}

	mockServer := &GBServer{
		ServerName: "mock-server-1",
		clusterMap: ClusterMap{
			participants: make(map[string]*Participant, 1),
		},
		gbClusterConfig: config,
	}

	keyValues := map[string]*Delta{
		"test:heartbeat": &Delta{KeyGroup: TEST_DKG, ValueType: INTERNAL_D, Key: _HEARTBEAT_, Version: 1640995200, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		"test:tcp":       &Delta{KeyGroup: TEST_DKG, ValueType: INTERNAL_D, Key: _ADDRESS_, Version: 1640995200, Value: []byte("127.0.0.0.1:8081")},
	}

	participant := &Participant{
		name:       mockServer.ServerName,
		keyValues:  keyValues,
		maxVersion: 1640995200,
	}

	mockServer.clusterMap.participants[mockServer.ServerName] = participant

	// Now test update heartbeat function

	self := mockServer.GetSelfInfo()
	heartbeatBytes := self.keyValues[_HEARTBEAT_].Value
	heartbeatInt := int64(binary.BigEndian.Uint64(heartbeatBytes))

	log.Printf("current heartbeat value = %v", heartbeatInt)
	log.Println("updating heartbeat...")
	time.Sleep(2 * time.Second)

	now := time.Now().Unix()
	mockServer.updateSelfInfo(now, func(participant *Participant, timeOfUpdate int64) error {
		err := updateHeartBeat(participant, timeOfUpdate)
		if err != nil {
			return err
		}
		return nil
	})

	heartbeatBytes = self.keyValues[_HEARTBEAT_].Value
	heartbeatInt = int64(binary.BigEndian.Uint64(heartbeatBytes))

	log.Printf("update heartbeat time: %v", heartbeatInt)

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
		mockServer.updateSelfInfo(time.Now().Unix(), func(participant *Participant, timeOfUpdate int64) error {
			err := updateHeartBeat(participant, timeOfUpdate)
			if err != nil {
				return err
			}
			return nil
		})
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

// Test Add GSA To Map error handling

func TestLiveGossip(t *testing.T) {

	//now := time.Now().Unix()

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbClusterConfig{
		SeedServers: []*Seeds{
			{
				Host: ip,
				Port: port,
			},
		},
		Cluster: &ClusterOptions{},
	}

	nodeConfig := &GbNodeConfig{}

	gbs, _ := NewServer("test-server", config, nodeConfig, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server", config, nodeConfig, "localhost", "8082", "8083", lc)
	gbs3, _ := NewServer("test-server", config, nodeConfig, "localhost", "8085", "8084", lc)

	go gbs.StartServer()
	time.Sleep(1 * time.Second)

	//testKV := map[string]*Delta{
	//	"test:TEST":        &Delta{keyGroup: TEST_DKG, key: "TEST", valueType: STRING_V, version: now, value: []byte("Hey - I am going to be the first string to be gossiped in GoferBroke :)")},
	//	"test:SECOND_TEST": &Delta{keyGroup: TEST_DKG, key: "SECOND_TEST", valueType: STRING_V, version: now, value: []byte("And I am the Second! Although no one will remember me :(")},
	//}

	go gbs2.StartServer()
	time.Sleep(1 * time.Second)

	//gbs2.clusterMapLock.Lock()
	//gbs2.clusterMap.participants[gbs2.ServerName].keyValues["SECOND_TEST"] = testKV["SECOND_TEST"]
	//gbs2.clusterMapLock.Unlock()

	go gbs3.StartServer()
	time.Sleep(1 * time.Second)

	//gbs.clusterMapLock.Lock()
	//gbs.clusterMap.participants[gbs.ServerName].keyValues["test:TEST"] = testKV["test:TEST"]
	//gbs.clusterMap.participants[gbs.ServerName].keyValues["test:SECOND_TEST"] = testKV["test:SECOND_TEST"]
	//gbs.clusterMapLock.Unlock()

	time.Sleep(6 * time.Second)

	gbs2.ServerContext.Done()
	gbs3.ServerContext.Done()
	gbs.ServerContext.Done()

	go gbs2.Shutdown()
	go gbs3.Shutdown()
	go gbs.Shutdown()

	time.Sleep(1 * time.Second)

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
	gbs3.logActiveGoRoutines()

	time.Sleep(2 * time.Second)

	if value, exists := gbs3.clusterMap.participants[gbs.ServerName].keyValues["test:TEST"]; exists {
		log.Printf("%s - value: %s", gbs3.ServerName, string(value.Value))
	} else {
		log.Printf("no value :(")
	}
	if value, exists := gbs3.clusterMap.participants[gbs.ServerName].keyValues["test:SECOND_TEST"]; exists {
		log.Printf("%s - value: %s", gbs3.ServerName, string(value.Value))
	} else {
		log.Printf("no value :(")
	}

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
