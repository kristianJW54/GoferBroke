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

	nodeA := &tmpDigest{
		name:       nodeAName,
		maxVersion: 1733134288,
	}
	nodeB := &tmpDigest{
		name:       nodeBName,
		maxVersion: 1733134288,
	}

	t.Logf("%s:%d", nodeA.name, nodeA.maxVersion)
	t.Logf("%s:%d", nodeB.name, nodeB.maxVersion)

	// Serialise will create wrapper array specifying type, length, size of digest
	// And also serialise elements within the digest

	// Create digest slice
	digest := []*tmpDigest{nodeA, nodeB}

	client := &gbClient{}

	// Call the serialiseDigest method
	serialized, err := client.serialiseDigest(digest)
	if err != nil {
		t.Fatalf("Failed to serialize digest: %v", err)
	}

	// Log the serialized data for inspection
	t.Logf("Serialized Digest: %v", serialized)
	t.Logf("lenght of serialized digest: %d", len(serialized))

	deserialized, err := client.deSerialiseDigest(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize digest: %v", err)
	}

	for _, value := range deserialized {
		t.Logf("%s:%v", value.name, value.maxVersion)
	}

}
