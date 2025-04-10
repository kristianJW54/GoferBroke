package Cluster

import (
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

// TODO Test build delta method for normal test, drop deltas, and no delta
func TestBuildDelta(t *testing.T) {

	// Gen 3 test servers
	// Main has newest deltas
	// Other two have old
	// Gen participant heap
	// Build delta

	var keyValues1 = map[string]*Delta{
		"TEST:key6":  {keyGroup: "TEST", key: "key6", valueType: INTERNAL_D, version: 1640995204, value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"TEST:key7":  {keyGroup: "TEST", key: "key7", valueType: INTERNAL_D, version: 1640995205, value: []byte("A")},
		"TEST:key8":  {keyGroup: "TEST", key: "key8", valueType: INTERNAL_D, version: 1640995206, value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"TEST:key9":  {keyGroup: "TEST", key: "key9", valueType: INTERNAL_D, version: 1640995207, value: []byte("😃 Emoji support test.")},
		"TEST:key10": {keyGroup: "TEST", key: "key10", valueType: INTERNAL_D, version: 1640995208, value: []byte("Another simple string.")},
		"TEST:key11": {keyGroup: "TEST", key: "key11", valueType: INTERNAL_D, version: 1640995209, value: []byte("This is test entry number eleven.")},
		"TEST:key12": {keyGroup: "TEST", key: "key12", valueType: INTERNAL_D, version: 1640995210, value: []byte("Entry twelve includes a longer message to check proper handling.")},
		"TEST:key13": {keyGroup: "TEST", key: "key13", valueType: INTERNAL_D, version: 1640995211, value: []byte("Testing entry for key 13, which is designed to be unique.")},
		"TEST:key14": {keyGroup: "TEST", key: "key14", valueType: INTERNAL_D, version: 1640995212, value: []byte("Another example message for the fourteenth key.")},
		"TEST:key15": {keyGroup: "TEST", key: "key15", valueType: INTERNAL_D, version: 1640995213, value: []byte("The fifteenth key's entry demonstrates variety in test data.")},
		"TEST:key16": {keyGroup: "TEST", key: "key16", valueType: INTERNAL_D, version: 1640995214, value: []byte("Testing with key16. Another sample text here.")},
		"TEST:key17": {keyGroup: "TEST", key: "key17", valueType: INTERNAL_D, version: 1640995215, value: []byte("Key 17: A different message string to showcase the test entry.")},
		"TEST:key18": {keyGroup: "TEST", key: "key18", valueType: INTERNAL_D, version: 1640995216, value: []byte("Eighteenth key value: a blend of letters and numbers: 1234567890.")},
		"TEST:key19": {keyGroup: "TEST", key: "key19", valueType: INTERNAL_D, version: 1640995217, value: []byte("Entry number 19 uses special characters: !@#$%^&*()")},
		"TEST:key20": {keyGroup: "TEST", key: "key20", valueType: INTERNAL_D, version: 1640995218, value: []byte("The final test key entry, providing closure for our sample.")},
	}

	var keyValues1Outdated = map[string]*Delta{
		"TEST:key6":  {keyGroup: "TEST", key: "key6", valueType: INTERNAL_D, version: 1640995203, value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"TEST:key7":  {keyGroup: "TEST", key: "key7", valueType: INTERNAL_D, version: 1640995204, value: []byte("A")},
		"TEST:key8":  {keyGroup: "TEST", key: "key8", valueType: INTERNAL_D, version: 1640995205, value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"TEST:key9":  {keyGroup: "TEST", key: "key9", valueType: INTERNAL_D, version: 1640995206, value: []byte("😃 Emoji support test.")},
		"TEST:key10": {keyGroup: "TEST", key: "key10", valueType: INTERNAL_D, version: 1640995207, value: []byte("Another simple string.")},
		"TEST:key11": {keyGroup: "TEST", key: "key11", valueType: INTERNAL_D, version: 1640995208, value: []byte("This is test entry number eleven.")},
		"TEST:key12": {keyGroup: "TEST", key: "key12", valueType: INTERNAL_D, version: 1640995209, value: []byte("Entry twelve includes a longer message to check proper handling.")},
		"TEST:key13": {keyGroup: "TEST", key: "key13", valueType: INTERNAL_D, version: 1640995210, value: []byte("Testing entry for key 13, which is designed to be unique.")},
		"TEST:key14": {keyGroup: "TEST", key: "key14", valueType: INTERNAL_D, version: 1640995211, value: []byte("Another example message for the fourteenth key.")},
		"TEST:key15": {keyGroup: "TEST", key: "key15", valueType: INTERNAL_D, version: 1640995212, value: []byte("The fifteenth key's entry demonstrates variety in test data.")},
		"TEST:key16": {keyGroup: "TEST", key: "key16", valueType: INTERNAL_D, version: 1640995213, value: []byte("Testing with key16. Another sample text here.")},
		"TEST:key17": {keyGroup: "TEST", key: "key17", valueType: INTERNAL_D, version: 1640995214, value: []byte("Key 17: A different message string to showcase the test entry.")},
		"TEST:key18": {keyGroup: "TEST", key: "key18", valueType: INTERNAL_D, version: 1640995215, value: []byte("Eighteenth key value: a blend of letters and numbers: 1234567890.")},
		"TEST:key19": {keyGroup: "TEST", key: "key19", valueType: INTERNAL_D, version: 1640995216, value: []byte("Entry number 19 uses special characters: !@#$%^&*()")},
		"TEST:key20": {keyGroup: "TEST", key: "key20", valueType: INTERNAL_D, version: 1640995217, value: []byte("The final test key entry, providing closure for our sample.")},
	}

	gbs := GenerateDefaultTestServer("server-1", keyValues1, 3)
	gbs2 := GenerateDefaultTestServer("server-2", keyValues1Outdated, 3)

	//gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, gbs2.ServerName)
	//gbs.clusterMap.participants[gbs2.ServerName] = &Participant{
	//	name:       gbs2.ServerName,
	//	keyValues:  keyValues1Outdated,
	//	maxVersion: gbs2.clusterMap.participants[gbs2.ServerName].maxVersion,
	//}
	//
	//gbs2.clusterMap.participantArray = append(gbs.clusterMap.participantArray, gbs.ServerName)
	//gbs2.clusterMap.participants[gbs.ServerName] = &Participant{
	//	name:       gbs.ServerName,
	//	keyValues:  keyValues1,
	//	maxVersion: gbs.clusterMap.participants[gbs.ServerName].maxVersion,
	//}

	for name, part := range gbs.clusterMap.participants {
		log.Printf("name = %s", name)
		log.Printf("part = %+v", part.name)
	}

	d, size, err := gbs2.generateDigest()
	if err != nil {
		t.Errorf("Error generating digest: %v", err)
	}

	//Modify digest if no error
	//newD, newSize, err := gbs.modifyDigest(d)
	//if err != nil {
	//	t.Errorf("Error generating digest: %v", err)
	//}

	remaining := DEFAULT_MAX_GSA - size

	_, fd, err := deSerialiseDigest(d)
	if err != nil {
		t.Errorf("Error de-serialising digest: %v", err)
	}

	for _, v := range *fd {
		log.Printf("v = %+v", v)
	}

	ph, err := gbs.generateParticipantHeap(gbs2.ServerName, fd)
	log.Printf("len of ph = %d", len(ph))

	delta, size, err := gbs.buildDelta(&ph, remaining)
	if err != nil {
		t.Errorf("Error building delta: %v", err)
	}

	log.Printf("size of delta = %v", size)

	for k, v := range delta {
		log.Printf("k = %v", k)
		for _, value := range v {
			log.Printf("value = %+v", value)
		}
	}

}

// TODO Delta heap test for ordering of deltas in most outdated first

func TestDeltaHeap(t *testing.T) {

	// Create keyValues with PBDelta messages
	var keyValues = map[string]*Delta{
		"TEST:key6":  {keyGroup: "TEST", key: "key6", valueType: INTERNAL_D, version: 1640995203, value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"TEST:key7":  {keyGroup: "TEST", key: "key7", valueType: INTERNAL_D, version: 1640995205, value: []byte("A")},
		"TEST:key8":  {keyGroup: "TEST", key: "key8", valueType: INTERNAL_D, version: 1640995206, value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"TEST:key9":  {keyGroup: "TEST", key: "key9", valueType: INTERNAL_D, version: 1640995207, value: []byte("😃 Emoji support test.")},
		"TEST:key10": {keyGroup: "TEST", key: "key10", valueType: INTERNAL_D, version: 1640995208, value: []byte("Another simple string.")},
	}

	dh := make(deltaHeap, 5)

	i := 0
	for _, value := range keyValues {

		dh[i] = &deltaQueue{
			key:      value.key,
			overload: false,
			version:  value.version,
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
	config := &GbConfig{
		SeedServers: map[string]Seeds{
			"seed1": {},
		},
		Internal: &InternalOptions{},
		Cluster:  &ClusterOptions{},
	}

	mockServer := &GBServer{
		ServerName: "mock-server-1",
		clusterMap: ClusterMap{
			participants: make(map[string]*Participant, 1),
		},
		gbConfig: config,
	}

	keyValues := map[string]*Delta{
		_HEARTBEAT_: &Delta{valueType: INTERNAL_D, key: _HEARTBEAT_, version: 1640995200, value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		_ADDRESS_:   &Delta{valueType: INTERNAL_D, key: _ADDRESS_, version: 1640995200, value: []byte("127.0.0.0.1:8081")},
	}

	participant := &Participant{
		name:       mockServer.ServerName,
		keyValues:  keyValues,
		maxVersion: 1640995200,
	}

	mockServer.clusterMap.participants[mockServer.ServerName] = participant

	// Now test update heartbeat function

	self := mockServer.getSelfInfo()
	heartbeatBytes := self.keyValues[_HEARTBEAT_].value
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

	heartbeatBytes = self.keyValues[_HEARTBEAT_].value
	heartbeatInt = int64(binary.BigEndian.Uint64(heartbeatBytes))

	log.Printf("update heartbeat time: %v", heartbeatInt)

}

func TestClusterMapLocks(t *testing.T) {

	// To test cluster map locks under high concurrency by having worker routines reading cluster map,
	// writing new participants and writing current participant value and updating self

	// Need tasks and workers to assign the tasks to
	// Tasks should be: Add participant, read cluster map, update cluster map, update self info

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: map[string]Seeds{
			"seed1": {},
		},
		Internal: &InternalOptions{},
		Cluster:  &ClusterOptions{},
	}

	mockServer := &GBServer{
		ServerName: "mock-server-1",
		clusterMap: ClusterMap{
			participants: make(map[string]*Participant, 1),
		},
		gbConfig: config,
	}

	keyValues := map[string]*Delta{
		_HEARTBEAT_: &Delta{valueType: INTERNAL_D, key: _HEARTBEAT_, version: 1640995200, value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		_ADDRESS_:   &Delta{valueType: INTERNAL_D, key: _ADDRESS_, version: 1640995200, value: []byte("127.0.0.0.1:8081")},
	}

	participant := &Participant{
		name:       mockServer.ServerName,
		keyValues:  keyValues,
		maxVersion: 1640995200,
	}

	mockServer.clusterMap.participants[mockServer.ServerName] = participant

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
		log.Printf("[Task] Reading cluster map")
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
			keyValues: keyValues,
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

// Use for output testing not actual testing
func TestGossSynAck(t *testing.T) {

	now := time.Now().Unix()

	node1KeyValues := map[string]*Delta{
		"key1": &Delta{key: "key1", valueType: STRING_V, version: now, value: []byte("I am a delta blissfully unaware as to how annoying I am to code")},
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now, value: []byte("Try to gossip about me and see what happens")},
	}

	node2KeyValues := map[string]*Delta{
		"key1": &Delta{key: "key1", valueType: STRING_V, version: now - 2, value: []byte("I am a delta blissfully")},
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now - 1, value: []byte("I Don't Like Gossipers")},
	}

	node1 := GenerateDefaultTestServer("node-1", node1KeyValues, 0)
	node2 := GenerateDefaultTestServer("node-2", node2KeyValues, 0)

	// Generate a digest from the lower version node
	d, _, err := node2.generateDigest()
	if err != nil {
		t.Errorf("generate digest failed: %v", err)
	}

	name, fd, err := deSerialiseDigest(d)
	if err != nil {
		t.Errorf("deSerialise digest failed: %v", err)
	}

	// Prepare GSA
	gsa, err := node1.prepareGossSynAck(name, fd)
	if err != nil {
		t.Errorf("prepareGossSynAck failed: %v", err)
	}

	newName, newFd, newCd, err := deserialiseGSA(gsa)
	if err != nil {
		t.Errorf("deserialise GSA failed: %v", err)
	}

	log.Printf("name = %s", newName)
	for _, f := range *newFd {
		log.Printf("digest check = %+v", f)
	}

	if newCd != nil {
		for _, c := range newCd.delta {
			for k, v := range c.keyValues {
				log.Printf("key = %s value = %s", k, v.value)
			}
		}
	}

}

func TestGSATwoNodes(t *testing.T) {

	now := time.Now().Unix()

	node1KeyValues := map[string]*Delta{
		"key1": &Delta{key: "key1", valueType: STRING_V, version: now, value: []byte("I am a delta blissfully unaware as to how annoying I am to code")},
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now, value: []byte("Try to gossip about me and see what happens")},
	}

	node2KeyValues := map[string]*Delta{
		"key1": &Delta{key: "key1", valueType: STRING_V, version: now - 2, value: []byte("I am a delta blissfully")},
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now - 1, value: []byte("I Don't Like Gossipers")},
	}

	node3KeyValues := map[string]*Delta{
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now - 4, value: []byte("One delta andy over here")},
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
			name: "baseline test - two nodes with both in map",
			node1: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-1", node1KeyValues, 0)

				gbs.clusterMap.participants["node-2"] = &Participant{
					name:       "node-2",
					keyValues:  node2KeyValues,
					maxVersion: node2KeyValues["key2"].version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

				return gbs
			}(),
			node2: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-2", node2KeyValues, 0)

				gbs.clusterMap.participants["node-1"] = &Participant{
					name:       "node-1",
					keyValues:  node1KeyValues,
					maxVersion: node1KeyValues["key2"].version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-1")

				return gbs

			}(),
			shouldHaveDelta:  false,
			participantCount: 0,
			digestCount:      2,
		},
		{
			name: "node-1 has an extra participant that node-2 doesn't know about",
			node1: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-1", node1KeyValues, 0)
				gbs.clusterMap.participants["node-2"] = &Participant{
					name:       "node-2",
					keyValues:  node2KeyValues,
					maxVersion: node2KeyValues["key2"].version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

				gbs.clusterMap.participants["node-3"] = &Participant{
					name:       "node-3",
					keyValues:  node3KeyValues,
					maxVersion: node3KeyValues["key2"].version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-3")

				return gbs

			}(),
			node2: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-2", node2KeyValues, 0)

				gbs.clusterMap.participants["node-1"] = &Participant{
					name:       "node-1",
					keyValues:  node1KeyValues,
					maxVersion: node1KeyValues["key2"].version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-1")

				return gbs

			}(),
			shouldHaveDelta:  true,
			participantCount: 1,
			digestCount:      3,
		},
		{
			name: "node-2 will have a lesser max version to node-1",
			node1: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-1", node1KeyValues, 0)

				gbs.clusterMap.participants["node-2"] = &Participant{
					name:       "node-2",
					keyValues:  node2KeyValues,
					maxVersion: node2KeyValues["key2"].version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

				return gbs

			}(),
			node2: func() *GBServer {

				gbs := GenerateDefaultTestServer("node-2", node2KeyValues, 0)
				gbs.clusterMap.participants[gbs.ServerName].maxVersion = now - 2

				gbs.clusterMap.participants["node-1"] = &Participant{
					name:       "node-1",
					keyValues:  node1KeyValues,
					maxVersion: node1KeyValues["key2"].version,
				}

				gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-1")

				return gbs

			}(),
			shouldHaveDelta:  false,
			participantCount: 0,
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

			for _, dv := range *fd {
				log.Printf("node 2 ---> digest sent = %+v", dv)
			}

			// Prepare GSA
			gsa, err := tt.node1.prepareGossSynAck(name, fd)
			if err != nil {
				t.Errorf("prepareGossSynAck failed: %v", err)
			}

			sender, newFd, newCd, err := deserialiseGSA(gsa)
			if err != nil {
				t.Errorf("deserialise GSA failed: %v", err)
			}

			log.Printf("%s received ------> name = %s", tt.node1.name, sender)
			for _, fdValue := range *newFd {
				log.Printf("digest check = %+v", fdValue)
			}

			if newCd != nil {
				for _, c := range newCd.delta {
					for k, v := range c.keyValues {
						log.Printf("key = %s value = %s", k, v.value)
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
				if len(newCd.delta) != tt.participantCount {
					t.Errorf("should have %d participants, got %d", tt.participantCount, len(newCd.delta))
				}
			}

		})
	}
}

func TestAddGSADeltaToMap(t *testing.T) {

	now := time.Now().Unix()

	node1KeyValues := map[string]*Delta{
		"key1": &Delta{key: "key1", valueType: STRING_V, version: now, value: []byte("I am a delta blissfully unaware as to how annoying I am to code")},
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now, value: []byte("Try to gossip about me and see what happens")},
	}

	node2KeyValues := map[string]*Delta{
		"key1": &Delta{key: "key1", valueType: STRING_V, version: now, value: []byte("I am a delta blissfully")},
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now, value: []byte("I Don't Like Gossipers")},
	}

	node3KeyValues := map[string]*Delta{
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now - 2, value: []byte("One delta andy over here - Im the newer version boi ;)")},
	}

	node3KeyValuesOutdated := map[string]*Delta{
		"key2": &Delta{key: "key2", valueType: STRING_V, version: now - 4, value: []byte("One delta andy over here")},
	}

	// Node 1

	gbs := GenerateDefaultTestServer("node-1", node1KeyValues, 0)

	gbs.clusterMap.participants["node-2"] = &Participant{
		name:       "node-2",
		keyValues:  node2KeyValues,
		maxVersion: node2KeyValues["key2"].version,
	}

	gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-2")

	gbs.clusterMap.participants["node-3"] = &Participant{
		name:       "node-3",
		keyValues:  node3KeyValues,
		maxVersion: node3KeyValues["key2"].version,
	}

	gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, "node-3")

	// Node 2

	gbs2 := GenerateDefaultTestServer("node-2", node2KeyValues, 0)
	gbs2.clusterMap.participants[gbs2.ServerName].maxVersion = now - 2

	gbs2.clusterMap.participants["node-1"] = &Participant{
		name:       "node-1",
		keyValues:  node1KeyValues,
		maxVersion: node1KeyValues["key2"].version,
	}

	gbs2.clusterMap.participantArray = append(gbs2.clusterMap.participantArray, "node-1")

	gbs2.clusterMap.participants["node-3"] = &Participant{
		name:       "node-3",
		keyValues:  node3KeyValuesOutdated,
		maxVersion: node3KeyValuesOutdated["key2"].version,
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

	for _, dv := range *fd {
		log.Printf("digest sent = %+v", dv)
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

	keyCheck := gbs2.clusterMap.participants["node-3"].keyValues["key2"]

	log.Printf("keyCheck: version %v - value %s", keyCheck.version, keyCheck.value)

}

func TestLiveGossip(t *testing.T) {

	now := time.Now().Unix()

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: map[string]Seeds{
			"seed1": {
				SeedIP:   ip,
				SeedPort: port,
			},
		},
		Internal: &InternalOptions{
			//disableGossip: true,
		},
		Cluster: &ClusterOptions{},
	}

	gbs, _ := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server", 2, config, "localhost", "8082", "8083", lc)
	gbs3, _ := NewServer("test-server", 3, config, "localhost", "8085", "8084", lc)

	go gbs.StartServer()
	time.Sleep(1 * time.Second)

	testKV := map[string]*Delta{
		"TEST":        &Delta{key: "TEST", valueType: STRING_V, version: now, value: []byte("Hey - I am going to be the first string to be gossiped in GoferBroke :)")},
		"SECOND_TEST": &Delta{key: "SECOND_TEST", valueType: STRING_V, version: now, value: []byte("And I am the Second! Although no one will remember me :(")},
	}

	go gbs2.StartServer()
	time.Sleep(1 * time.Second)

	//gbs2.clusterMapLock.Lock()
	//gbs2.clusterMap.participants[gbs2.ServerName].keyValues["SECOND_TEST"] = testKV["SECOND_TEST"]
	//gbs2.clusterMapLock.Unlock()

	go gbs3.StartServer()
	time.Sleep(1 * time.Second)

	gbs.clusterMapLock.Lock()
	gbs.clusterMap.participants[gbs.ServerName].keyValues["TEST"] = testKV["TEST"]
	gbs.clusterMap.participants[gbs.ServerName].keyValues["SECOND_TEST"] = testKV["SECOND_TEST"]
	gbs.clusterMapLock.Unlock()

	time.Sleep(6 * time.Second)

	gbs2.serverContext.Done()
	gbs3.serverContext.Done()
	gbs.serverContext.Done()

	go gbs2.Shutdown()
	go gbs3.Shutdown()
	go gbs.Shutdown()

	time.Sleep(1 * time.Second)

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
	gbs3.logActiveGoRoutines()

	time.Sleep(2 * time.Second)

	if value, exists := gbs3.clusterMap.participants[gbs.ServerName].keyValues["TEST"]; exists {
		log.Printf("%s - value: %s", gbs3.ServerName, string(value.value))
	} else {
		log.Printf("no value :(")
	}
	if value, exists := gbs3.clusterMap.participants[gbs.ServerName].keyValues["SECOND_TEST"]; exists {
		log.Printf("%s - value: %s", gbs3.ServerName, string(value.value))
	} else {
		log.Printf("no value :(")
	}

}
