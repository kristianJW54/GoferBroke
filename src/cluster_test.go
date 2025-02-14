package src

import (
	"encoding/binary"
	"log"
	"testing"
	"time"
)

func TestUpdateHeartBeat(t *testing.T) {

	mockServer := &GBServer{
		ServerName: "mock-server-1",
		clusterMap: ClusterMap{
			participants: make(map[string]*Participant, 1),
			participantQ: make(participantHeap, 0),
		},
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

func TestUpdateClusterMapLocks(t *testing.T) {

	// To test cluster map locks under high concurrency by having worker routines reading cluster map,
	// writing new participants and writing current participant value and updating self

	// Need tasks and workers to assign the tasks to
	// Tasks should be: Add participant, read cluster map, update cluster map, update self info

}
