package cluster

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type grTracking struct {
	numRoutines  int64
	index        int64
	trackingFlag atomic.Value
	grWg         *sync.WaitGroup
	routineMap   sync.Map
}

func (g *grTracking) startGoRoutine(serverName, name string, f func()) {
	// Add to the WaitGroup to track the goroutine
	g.grWg.Add(1)

	// Check if tracking is enabled
	if tracking, ok := g.trackingFlag.Load().(bool); ok && tracking {
		// Generate a new unique ID for the goroutine
		id := atomic.AddInt64(&g.index, 1)
		atomic.AddInt64(&g.numRoutines, 1)

		// Log the start of the goroutine
		//log.Printf("%s Starting go-routine %v - %v", serverName, name, id)

		// Store the routine's information in the map
		g.routineMap.Store(id, name)

		// Launch the goroutine
		go func() {
			defer g.grWg.Done()
			defer func() {

				// TODO Think about if we actually want to recover - we should not be having routine problems
				// Recover from any panic that may occur in the goroutine
				//if r := recover(); r != nil {
				//	fmt.Printf("%s Recovered panic in goroutine %s: %v\n", serverName, name, r)
				//}

				// If tracking is enabled, decrement the number of routines and remove from the map
				atomic.AddInt64(&g.numRoutines, -1)
				g.routineMap.Delete(id)
				//log.Printf("%s Ending go-routine %v - %v", serverName, name, id)

			}()

			// Run the provided function for the goroutine
			f()
		}()
	} else {
		go func() {
			f()
		}()
	}

}

func (g *grTracking) logActiveGoRoutines() {
	log.Printf("Go routines left in tracker: %v", atomic.LoadInt64(&g.numRoutines))
	log.Println("Go routines in tracker:")

	g.routineMap.Range(func(key, value interface{}) bool {
		log.Printf("Goroutine -- %v %s\n", key, value)
		return true // continue iteration
	})
}

func percMakeup(known, want int) int {
	if want == 0 {
		return 0 // Prevent division by zero
	}
	return (known * 100) / want
}

//==========================================================================================

// For tests

func int64ToBytes(n int64) []byte {
	buf := make([]byte, 8) // Allocate 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

var heartBeatKV = map[string]*Delta{
	MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_): {KeyGroup: SYSTEM_DKG, Key: _HEARTBEAT_, ValueType: D_INT64_TYPE, Version: 1640995204, Value: int64ToBytes(1640995204)},
}

var keyValues1 = map[string]*Delta{
	"TEST:key6":  {KeyGroup: "TEST", Key: "key6", ValueType: INTERNAL_D, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
	"TEST:key7":  {KeyGroup: "TEST", Key: "key7", ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("A")},
	"TEST:key8":  {KeyGroup: "TEST", Key: "key8", ValueType: INTERNAL_D, Version: 1640995206, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
	"TEST:key9":  {KeyGroup: "TEST", Key: "key9", ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("😃 Emoji support test.")},
	"TEST:key10": {KeyGroup: "TEST", Key: "key10", ValueType: INTERNAL_D, Version: 1640995208, Value: []byte("Another simple string.")},
}

var keyValues1LowerVersion = map[string]*Delta{
	"TEST:key6":  {KeyGroup: "TEST", Key: "key6", ValueType: INTERNAL_D, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
	"TEST:key7":  {KeyGroup: "TEST", Key: "key7", ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("A")},
	"TEST:key8":  {KeyGroup: "TEST", Key: "key8", ValueType: INTERNAL_D, Version: 1640995201, Value: []byte("Test serialization with repeated values.")},
	"TEST:key9":  {KeyGroup: "TEST", Key: "key9", ValueType: INTERNAL_D, Version: 1640995202, Value: []byte("😃")},
	"TEST:key10": {KeyGroup: "TEST", Key: "key10", ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("Another string")},
}

var addressTestingKVs = map[string]*Delta{
	"address:tcp": {KeyGroup: ADDR_DKG, Key: _ADDRESS_, ValueType: ADDR_V, Version: 1640995204, Value: []byte("127.0.0.1")},
}

var multipleAddressTestingKVs = map[string]*Delta{
	"address:tcp":   {KeyGroup: ADDR_DKG, Key: _ADDRESS_, ValueType: ADDR_V, Version: 1640995204, Value: []byte("127.0.0.1")},
	"address:CLOUD": {KeyGroup: ADDR_DKG, Key: "CLOUD", ValueType: ADDR_V, Version: 1640995204, Value: []byte("137.184.248.0")},
	"address:DNS":   {KeyGroup: ADDR_DKG, Key: "DNS", ValueType: ADDR_V, Version: 1640995204, Value: []byte("example.com")},
}

var keyValues2 = map[string]*Delta{
	"address:tcp":        {KeyGroup: ADDR_DKG, Key: _ADDRESS_, ValueType: ADDR_V, Version: 1640995204, Value: []byte("127.0.0.1")},
	"address:NODE_CONNS": {KeyGroup: ADDR_DKG, Key: _NODE_CONNS_, ValueType: NUM_NODE_CONN_V, Version: 1640995205, Value: []byte{0}},
	"address:HEARTBEAT":  {KeyGroup: ADDR_DKG, Key: _HEARTBEAT_, ValueType: HEARTBEAT_V, Version: 1640995206, Value: int64ToBytes(1640995206)},
}

// TODO Make another one of these but with config

func GenerateDefaultTestServer(serverName string, kv map[string]*Delta, numParticipants int) *GBServer {

	if numParticipants == 0 {
		numParticipants = 1
	}

	// Mock server setup
	gbs := &GBServer{
		clusterMap: ClusterMap{
			participants:     make(map[string]*Participant, numParticipants),
			participantArray: make([]string, numParticipants),
		},
		ServerName: serverName,
		gbClusterConfig: &GbClusterConfig{
			SeedServers: []Seeds{
				{},
			},
			Cluster: &ClusterOptions{},
		},
		gbNodeConfig: &GbNodeConfig{
			Internal: &InternalOptions{},
		},
	}

	maxV := int64(0)

	for _, value := range kv {
		if value.Version > maxV {
			maxV = value.Version
		}
	}

	gbs.numNodeConnections = int64(numParticipants)

	mainPart := &Participant{
		name:       serverName,
		keyValues:  kv,
		maxVersion: maxV,
	}

	gbs.clusterMap.participantArray[0] = mainPart.name

	gbs.clusterMap.participants[gbs.ServerName] = mainPart

	if numParticipants == 1 {
		return gbs
	}

	for i := 1; i < numParticipants; i++ {

		participantName := fmt.Sprintf("node-test-%d", i)
		gbs.name = participantName

		// Create a participant
		participant := &Participant{
			name:       participantName,
			keyValues:  kv,
			maxVersion: maxV,
		}

		gbs.clusterMap.participantArray[i] = gbs.name

		// Add participant to the ClusterMap
		gbs.clusterMap.participants[participantName] = participant

	}

	return gbs

}

func GenerateDefaultTestServerWithDiff(serverName string, kv, diff map[string]*Delta, numParticipants int) *GBServer {

	if numParticipants == 0 {
		numParticipants = 1
	}

	// Mock server setup
	gbs := &GBServer{
		clusterMap: ClusterMap{
			participants:     make(map[string]*Participant, numParticipants),
			participantArray: make([]string, numParticipants),
		},
		ServerName: serverName,
	}

	maxV := int64(0)

	for _, value := range kv {
		if value.Version > maxV {
			maxV = value.Version
		}
	}

	maxVDiff := int64(0)

	for _, value := range diff {
		if value.Version > maxVDiff {
			maxVDiff = value.Version
		}
	}

	gbs.numNodeConnections = int64(numParticipants)

	mainPart := &Participant{
		name:       serverName,
		keyValues:  diff,
		maxVersion: maxVDiff,
	}

	gbs.clusterMap.participantArray[0] = mainPart.name

	gbs.clusterMap.participants[gbs.ServerName] = mainPart

	if numParticipants == 1 {
		return gbs
	}

	for i := 1; i < numParticipants; i++ {

		participantName := fmt.Sprintf("node-test-%d", i)
		gbs.name = participantName

		// Create a participant
		participant := &Participant{
			name:       participantName,
			keyValues:  diff,
			maxVersion: maxVDiff,
		}

		gbs.clusterMap.participantArray[i] = gbs.name

		// Add participant to the ClusterMap
		gbs.clusterMap.participants[participantName] = participant

	}

	return gbs

}
