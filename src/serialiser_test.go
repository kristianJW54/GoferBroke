package src

import (
	"fmt"
	"log"
	"net"
	"testing"
	"time"
)

func TestSerialiseDigest(t *testing.T) {

	// Creating node names
	timeCode := time.Now().Unix()
	nodeAName := fmt.Sprintf("node-a%d", timeCode)
	nodeBName := fmt.Sprintf("node-b%d", timeCode)

	nodeA := &fullDigest{
		nodeName:    nodeAName,
		maxVersion:  1733134288,
		keyVersions: make(map[string]int64, 4),
	}

	nodeA.keyVersions["key1"] = time.Now().Unix() - 100
	nodeA.keyVersions["key2"] = time.Now().Unix() - 200
	nodeA.keyVersions["key3"] = time.Now().Unix() - 300
	nodeA.keyVersions["key4"] = time.Now().Unix() - 400

	nodeA.vi = make([]string, 4)
	nodeA.vi[0] = "key1"
	nodeA.vi[1] = "key2"
	nodeA.vi[2] = "key3"
	nodeA.vi[3] = "key4"

	nodeB := &fullDigest{
		nodeName:    nodeBName,
		maxVersion:  1733134288,
		keyVersions: make(map[string]int64, 4),
	}

	nodeB.keyVersions["key1"] = time.Now().Unix() - 500
	nodeB.keyVersions["key2"] = time.Now().Unix() - 600
	nodeB.keyVersions["key3"] = time.Now().Unix() - 700
	nodeB.keyVersions["key4"] = time.Now().Unix() - 800

	nodeB.vi = make([]string, 4)
	nodeB.vi[0] = "key1"
	nodeB.vi[1] = "key2"
	nodeB.vi[2] = "key3"
	nodeB.vi[3] = "key4"

	t.Logf("%s:%d", nodeA.nodeName, nodeA.maxVersion)
	t.Logf("%s:%d", nodeB.nodeName, nodeB.maxVersion)

	// Serialise will create wrapper array specifying type, length, size of digest
	// And also serialise elements within the digest

	// Create digest slice
	digest := []*fullDigest{nodeA, nodeB}

	// Call the serialiseDigest method
	serialized, err := serialiseDigest(digest)
	if err != nil {
		t.Fatalf("Failed to serialize digest: %v", err)
	}

	// Log the serialized data for inspection
	t.Logf("Serialized Digest: %v", serialized)
	t.Logf("lenght of serialized digest: %d", len(serialized))

	deserialized, err := deSerialiseDigest(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize digest: %v", err)
	}

	for _, value := range deserialized {
		t.Logf("%s:%v", value.nodeName, value.maxVersion)
		for k, v := range value.keyVersions {
			t.Logf("%s:%v", k, v)
		}
	}

}

func BenchmarkSerialiseAndDeserialiseDigest(b *testing.B) {
	// Setup: Create example digests
	timeCode := time.Now().Unix()
	nodeAName := fmt.Sprintf("node-a%d", timeCode)
	nodeBName := fmt.Sprintf("node-b%d", timeCode)

	nodeA := &fullDigest{
		nodeName:    nodeAName,
		maxVersion:  1733134288,
		keyVersions: make(map[string]int64, 4),
		vi:          make([]string, 4),
	}

	nodeA.keyVersions["key1"] = time.Now().Unix() - 100
	nodeA.keyVersions["key2"] = time.Now().Unix() - 200
	nodeA.keyVersions["key3"] = time.Now().Unix() - 300
	nodeA.keyVersions["key4"] = time.Now().Unix() - 400

	nodeA.vi[0] = "key1"
	nodeA.vi[1] = "key2"
	nodeA.vi[2] = "key3"
	nodeA.vi[3] = "key4"

	nodeB := &fullDigest{
		nodeName:    nodeBName,
		maxVersion:  1733134288,
		keyVersions: make(map[string]int64, 4),
		vi:          make([]string, 4),
	}

	nodeB.keyVersions["key1"] = time.Now().Unix() - 500
	nodeB.keyVersions["key2"] = time.Now().Unix() - 600
	nodeB.keyVersions["key3"] = time.Now().Unix() - 700
	nodeB.keyVersions["key4"] = time.Now().Unix() - 800

	nodeB.vi[0] = "key1"
	nodeB.vi[1] = "key2"
	nodeB.vi[2] = "key3"
	nodeB.vi[3] = "key4"

	digest := []*fullDigest{nodeA, nodeB}

	// Benchmark serialization
	b.Run("SerializeDigest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := serialiseDigest(digest)
			if err != nil {
				b.Fatalf("Failed to serialize digest: %v", err)
			}
		}
	})

	// Prepare serialized data for deserialization benchmark
	serialized, err := serialiseDigest(digest)
	if err != nil {
		b.Fatalf("Failed to serialize digest: %v", err)
	}

	// Benchmark deserialization
	b.Run("DeserializeDigest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := deSerialiseDigest(serialized)
			if err != nil {
				b.Fatalf("Failed to deserialize digest: %v", err)
			}
		}
	})
}

// Helper function to create a Delta
func createDelta(valueType int, version int64, value string) *Delta {
	return &Delta{
		valueType: valueType,
		version:   version,
		value:     []byte(value),
	}
}

func TestSerialiseDelta(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   ip,
				SeedPort: port,
			},
		},
	}

	//log.Println(config)

	gbs := NewServer("test-server", config, "localhost", "8081", "8080", lc)

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	log.Printf("p name = %v | values %v", gbs.selfInfo.name, gbs.selfInfo.keyValues[_ADDRESS_])

	//// Creating node names
	//timeCode := time.Now().Unix()
	//nodeAName := fmt.Sprintf("node-a%d", timeCode)
	//nodeBName := fmt.Sprintf("node-b%d", timeCode)
	//
	//// Create test clusterDelta
	//testClusterDelta := &clusterDelta{
	//	delta: map[string]*tmpParticipant{
	//		nodeAName: {
	//			keyValues: map[string]*Delta{
	//				_ADDRESS_:   createDelta(0, timeCode, "192.168.0.1"),
	//				_CPU_USAGE_: createDelta(0, timeCode, "45.3%"),
	//				"ACCOUNT":   createDelta(0, timeCode, "{user123:password:thisismypassword"),
	//			},
	//			vi: []string{_ADDRESS_, _CPU_USAGE_, "ACCOUNT"}, // Store the value indices here
	//		},
	//		nodeBName: {
	//			keyValues: map[string]*Delta{
	//				_ADDRESS_:   createDelta(0, timeCode, "192.168.0.2"),
	//				_CPU_USAGE_: createDelta(0, timeCode, "55.7%"),
	//				"ACCOUNT":   createDelta(0, timeCode, "{user123:password:thisismypassword"),
	//			},
	//			vi: []string{_ADDRESS_, _CPU_USAGE_, "ACCOUNT"}, // Store the value indices here
	//		},
	//	},
	//}
	//
	//// Define value index and participant index for serialization
	//pi := []string{nodeAName, nodeBName} // Participant index, referencing the node names

	// Serialise the testClusterDelta with the participant index and value index
	gbs.clusterMapLock.RLock()
	cereal, err := gbs.serialiseClusterDelta(nil)
	if err != nil {
		t.Fatalf("Failed to serialise cluster delta: %v", err)
	}
	gbs.clusterMapLock.RUnlock()

	log.Println("cereal ==== ", cereal)

	// De-serialise the serialized data back into a new clusterDelta
	cd, err := deserialiseDelta(cereal)
	if err != nil {
		t.Fatalf("Failed to deserialise cluster delta: %v", err)
	}

	// Print the deserialized structure for verification
	t.Log("Deserialized Cluster Delta:")
	for key, value := range cd.delta {
		t.Logf("name = %s", key)
		for k, value := range value.keyValues {
			t.Logf("key-%s(%d)(%s)", k, value.valueType, value.value)
		}
	}

}

// TODO Test this with protobuf

func BenchmarkSerialiseDelta(b *testing.B) {
	// Mock server setup with an empty Server struct
	gbs := &GBServer{
		selfInfo: &Participant{
			name:      "test-server",
			keyValues: make(map[string]*Delta),
		},
		clusterMap: ClusterMap{
			participants: make(map[string]*Participant),
			partIndex:    []string{"node-1", "node-2", "node-3"}, // Manually entering participants
		},
	}

	// Manually enter mock data into the cluster map with larger, realistic values
	gbs.clusterMap.participants["node-1"] = &Participant{
		name: "node-1",
		keyValues: map[string]*Delta{
			"newAccount.log":        createDelta(1, 1, `{"username": "user123", "password": "SecurePassword123!", "email": "user123@example.com", "created_at": "2024-12-27T12:00:00Z", "address": {"street": "123 Elm St", "city": "New York", "state": "NY", "zip": "10001"}}`),
			"accountChange.user123": createDelta(1, 2, `{"username": "user123", "password": "UpdatedPassword456!", "email": "user123_new@example.com", "last_login": "2024-12-27T14:00:00Z", "preferences": {"theme": "dark", "notifications": true}}`),
		},
		valueIndex: []string{"newAccount.log", "accountChange.user123"},
		maxVersion: 2,
	}

	gbs.clusterMap.participants["node-2"] = &Participant{
		name: "node-2",
		keyValues: map[string]*Delta{
			"newAccount.log":        createDelta(1, 3, `{"username": "user124", "password": "SuperSecure123", "email": "user124@example.com", "created_at": "2024-12-26T09:30:00Z", "address": {"street": "456 Maple Ave", "city": "Chicago", "state": "IL", "zip": "60601"}}`),
			"accountChange.user123": createDelta(1, 4, `{"username": "user124", "password": "RevisedSecurePass789", "email": "user124_new@example.com", "last_login": "2024-12-26T10:00:00Z", "preferences": {"theme": "light", "notifications": false}}`),
		},
		valueIndex: []string{"newAccount.log", "accountChange.user123"},
		maxVersion: 4,
	}

	gbs.clusterMap.participants["node-3"] = &Participant{
		name: "node-3",
		keyValues: map[string]*Delta{
			"newAccount.log":        createDelta(1, 5, `{"username": "user125", "password": "Password123!", "email": "user125@example.com", "created_at": "2024-12-25T15:00:00Z", "address": {"street": "789 Oak Dr", "city": "Los Angeles", "state": "CA", "zip": "90001"}}`),
			"accountChange.user123": createDelta(1, 6, `{"username": "user125", "password": "UpdatedPassword321!", "email": "user125_new@example.com", "last_login": "2024-12-25T17:00:00Z", "preferences": {"theme": "auto", "notifications": true}}`),
		},
		valueIndex: []string{"newAccount.log", "accountChange.user123"},
		maxVersion: 6,
	}

	// Now that the server and participants are set up, start benchmarking
	b.ResetTimer() // Reset the timer to avoid setup time affecting the benchmark
	b.StartTimer()

	// Serialize delta for each participant
	for i := 0; i < b.N; i++ { // Iterate multiple times for better accuracy
		for _, participant := range gbs.clusterMap.partIndex {
			// Only serialize the name and keyValues delta
			_, err := gbs.serialiseClusterDelta(nil)
			if err != nil {
				b.Fatalf("Failed to serialize delta for participant %v: %v", participant, err)
			}
		}
	}

	b.StopTimer()
}

func BenchmarkMapIteration(b *testing.B) {
	m := make(map[string]int)
	for i := 0; i < 100; i++ {
		m[fmt.Sprintf("participant%d", i)] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k, v := range m {
			_ = k
			_ = v
		}
	}
}

func BenchmarkSliceIteration(b *testing.B) {
	s := make([]string, 100)
	for i := 0; i < 100; i++ {
		s[i] = fmt.Sprintf("participant%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range s {
			_ = v
		}
	}
}
