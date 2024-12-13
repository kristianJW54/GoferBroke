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

	nodeA := &clusterDigest{
		name:       nodeAName,
		maxVersion: 1733134288,
	}
	nodeB := &clusterDigest{
		name:       nodeBName,
		maxVersion: 1733134288,
	}

	t.Logf("%s:%d", nodeA.name, nodeA.maxVersion)
	t.Logf("%s:%d", nodeB.name, nodeB.maxVersion)

	// Serialise will create wrapper array specifying type, length, size of digest
	// And also serialise elements within the digest

	// Create digest slice
	digest := []*clusterDigest{nodeA, nodeB}

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
		t.Logf("%s:%v", value.name, value.maxVersion)
	}

}

// Helper function to create a Delta
func createDelta(valueType int, version int64, value string) *tmpDelta {
	return &tmpDelta{
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
				keyValues: map[int]*tmpDelta{
					ADDR_V:      createDelta(0, timeCode, "192.168.0.1"),
					CPU_USAGE_V: createDelta(0, timeCode, "45.3%"),
				},
			},
			nodeBName: {
				keyValues: map[int]*tmpDelta{
					ADDR_V:      createDelta(0, timeCode, "192.168.0.2"),
					CPU_USAGE_V: createDelta(0, timeCode, "55.7%"),
				},
			},
		},
	}

	// Print the test structure for verification
	for key, value := range testClusterDelta.delta {
		t.Logf("name = %s", key)
		for k, value := range value.keyValues {
			t.Logf("(key-%d)(%d)(%s)", k, value.valueType, value.value)
		}
	}

	cereal, err := serialiseClusterDelta(testClusterDelta)
	if err != nil {
		t.Fatalf("Failed to serialise cluster delta: %v", err)
	}

	// De-serialise

	cd, err := deserialiseDelta(cereal)
	if err != nil {
		t.Fatalf("Failed to deserialise cluster delta: %v", err)
	}

	for key, value := range cd.delta {
		t.Logf("name = %s", key)
		for k, value := range value.keyValues {
			t.Logf("key-%d(%d)(%s)", k, value.valueType, value.value)
		}
	}

}

func BenchmarkSerialiseDelta(b *testing.B) {
	// Creating node names
	timeCode := time.Now().Unix()
	nodeAName := fmt.Sprintf("node-a%d", timeCode)
	nodeBName := fmt.Sprintf("node-b%d", timeCode)

	// Create test clusterDelta
	testClusterDelta := &clusterDelta{
		delta: map[string]*tmpParticipant{
			nodeAName: {
				keyValues: map[int]*tmpDelta{
					1: createDelta(1, timeCode, "192.168.0.1"),
					2: createDelta(2, timeCode, "45.3%"),
				},
			},
			nodeBName: {
				keyValues: map[int]*tmpDelta{
					1: createDelta(1, timeCode, "192.168.0.2"),
					2: createDelta(2, timeCode, "55.7%"),
				},
			},
		},
	}

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := serialiseClusterDelta(testClusterDelta)
		if err != nil {
			b.Fatalf("Failed to serialise cluster delta: %v", err)
		}
	}
}
