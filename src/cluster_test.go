package src

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"log"
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

// TODO Delta heap test for ordering of deltas in most outdated first

func TestDeltaHeap(t *testing.T) {

	// Create keyValues with PBDelta messages
	keyValues := map[string]*Delta{
		"key6":  {valueType: INTERNAL_D, version: 1640995205, value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"key7":  {valueType: INTERNAL_D, version: 1640995207, value: []byte("A")},
		"key8":  {valueType: INTERNAL_D, version: 1640995206, value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"key9":  {valueType: INTERNAL_D, version: 1640995203, value: []byte("😃 Emoji support test.")},
		"key10": {valueType: INTERNAL_D, version: 1640995208, value: []byte("Another simple string.")},
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
