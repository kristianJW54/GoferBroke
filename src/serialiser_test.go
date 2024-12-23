package src

import (
	"fmt"
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

	// Creating node names
	timeCode := time.Now().Unix()
	nodeAName := fmt.Sprintf("node-a%d", timeCode)
	nodeBName := fmt.Sprintf("node-b%d", timeCode)

	// Create test clusterDelta
	testClusterDelta := &clusterDelta{
		delta: map[string]*tmpParticipant{
			nodeAName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.1"),
					_CPU_USAGE_: createDelta(0, timeCode, "45.3%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			nodeBName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.2"),
					_CPU_USAGE_: createDelta(0, timeCode, "55.7%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
		},
	}

	// Define value index and participant index for serialization
	pi := []string{nodeAName, nodeBName} // Participant index, referencing the node names

	// Print the test structure for verification
	for key, value := range testClusterDelta.delta {
		t.Logf("name = %s", key)
		for k, value := range value.keyValues {
			t.Logf("(key-%s)(%d)(%s)", k, value.valueType, value.value)
		}
	}

	// Serialise the testClusterDelta with the participant index and value index
	cereal, err := serialiseClusterDelta(testClusterDelta, pi)
	if err != nil {
		t.Fatalf("Failed to serialise cluster delta: %v", err)
	}

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

func BenchmarkSerialiseDelta(b *testing.B) {
	// Creating node names
	timeCode := time.Now().Unix()
	nodeAName := fmt.Sprintf("node-a%d", timeCode)
	nodeBName := fmt.Sprintf("node-b%d", timeCode)
	nodeCName := fmt.Sprintf("node-c%d", timeCode)
	nodeDName := fmt.Sprintf("node-d%d", timeCode)
	nodeEName := fmt.Sprintf("node-e%d", timeCode)
	nodeFName := fmt.Sprintf("node-f%d", timeCode)
	nodeGName := fmt.Sprintf("node-g%d", timeCode)
	nodeHName := fmt.Sprintf("node-h%d", timeCode)
	nodeIName := fmt.Sprintf("node-i%d", timeCode)
	nodeJName := fmt.Sprintf("node-j%d", timeCode)

	// Create test clusterDelta with 10 participants
	testClusterDelta := &clusterDelta{
		delta: map[string]*tmpParticipant{
			// Participant 1
			nodeAName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.1"),
					_CPU_USAGE_: createDelta(0, timeCode, "45.3%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 2
			nodeBName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.2"),
					_CPU_USAGE_: createDelta(0, timeCode, "55.7%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 3
			nodeCName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.3"),
					_CPU_USAGE_: createDelta(0, timeCode, "65.2%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 4
			nodeDName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.4"),
					_CPU_USAGE_: createDelta(0, timeCode, "72.8%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 5
			nodeEName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.5"),
					_CPU_USAGE_: createDelta(0, timeCode, "30.9%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 6
			nodeFName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.6"),
					_CPU_USAGE_: createDelta(0, timeCode, "62.4%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 7
			nodeGName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.7"),
					_CPU_USAGE_: createDelta(0, timeCode, "80.5%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 8
			nodeHName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.8"),
					_CPU_USAGE_: createDelta(0, timeCode, "90.6%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 9
			nodeIName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.9"),
					_CPU_USAGE_: createDelta(0, timeCode, "50.7%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
			// Participant 10
			nodeJName: {
				keyValues: map[string]*Delta{
					_ADDRESS_:   createDelta(0, timeCode, "192.168.0.10"),
					_CPU_USAGE_: createDelta(0, timeCode, "33.3%"),
				},
				vi: []string{_ADDRESS_, _CPU_USAGE_}, // Store the value indices here
			},
		},
	}

	// Define value index and participant index for serialization
	pi := []string{
		nodeAName, nodeBName, nodeCName, nodeDName, nodeEName,
		nodeFName, nodeGName, nodeHName, nodeIName, nodeJName,
	} // Participant index, referencing the node names

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := serialiseClusterDelta(testClusterDelta, pi)
		if err != nil {
			b.Fatalf("Failed to serialise cluster delta: %v", err)
		}
	}
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
