package src

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

// Test discovery by test config and turning off gossip

func TestParticipantHeap(t *testing.T) {

	gbs := GenerateDefaultTestServer(keyValues1, 5)

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
	err := mockServer.updateSelfInfo(now, func(participant *Participant, timeOfUpdate int64) error {
		err := updateHeartBeat(participant, timeOfUpdate)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Errorf("update heartbeat failed: %v", err)
	}

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
		err := mockServer.updateSelfInfo(time.Now().Unix(), func(participant *Participant, timeOfUpdate int64) error {
			err := updateHeartBeat(participant, timeOfUpdate)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Errorf("update heartbeat failed: %v", err)
		}
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

func TestGSA(t *testing.T) {

	gbs := GenerateDefaultTestServer(keyValues1, 5)
	gbs2 := GenerateDefaultTestServer(keyValues1, 5)

	part1 := gbs.clusterMap.participants[gbs.ServerName]
	part1.maxVersion = 1640995208
	part2 := gbs2.clusterMap.participants[gbs2.ServerName]
	part2.maxVersion = 1640995208

	// Generate a digest from the lower version node
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
		HandleError(err, func(gbError []*GBError) {

			gbErr := gbError[0]
			if errors.Is(err, gbErr) {
				log.Printf("We have an error to switch on %v", gbErr)
				// This is where we would switch our strategy
			} else {
				t.Errorf("prepareGossSynAck failed: %v", err)
			}

		})
		return
	}

	newName, newFd, newCd, err := deserialiseGSA(gsa)
	if err != nil {
		t.Errorf("deserialise GSA failed: %v", err)
	}

	log.Printf("name = %s", newName)
	for _, f := range *newFd {
		log.Printf("digest check = %+v", f)
	}

	for _, c := range newCd.delta {
		for k, v := range c.keyValues {
			log.Printf("key = %s value = %s", k, v.value)
		}
	}

}
