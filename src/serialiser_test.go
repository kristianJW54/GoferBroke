package src

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestSerialiseD(t *testing.T) {
	// Creating node names
	timeCode := time.Now().Unix()
	nodeAName := fmt.Sprintf("node-a%d", timeCode)
	nodeBName := fmt.Sprintf("node-b%d", timeCode)

	// Creating keys
	key1 := uint8(1)
	key2 := uint8(4)

	// Creating versions
	const (
		time1  = "2024-01-12 19:00:30"
		time2  = "2024-01-12 15:00:40"
		layout = "2006-01-02 15:04:05"
	)

	version1, err := time.Parse(layout, time1)
	if err != nil {
		t.Fatalf("Failed to parse time1: %v", err)
	}
	version2, err := time.Parse(layout, time2)
	if err != nil {
		t.Fatalf("Failed to parse time2: %v", err)
	}

	// Convert versions to Unix timestamps
	version1Unix := version1.Unix()
	version2Unix := version2.Unix()

	// Create a buffer for the serialized message
	var buf bytes.Buffer

	// Write header
	buf.WriteByte(1)                                // Protocol version
	binary.Write(&buf, binary.BigEndian, uint16(0)) // Placeholder for message length
	binary.Write(&buf, binary.BigEndian, uint16(4)) // Total items (Node A key1 + key2, Node B key1 + key2)

	// Serialize Node A
	binary.Write(&buf, binary.BigEndian, uint16(len(nodeAName))) // Node A name length
	buf.WriteString(nodeAName)                                   // Node A name
	// Node A keys and versions
	buf.WriteByte(key1)
	binary.Write(&buf, binary.BigEndian, uint64(version1Unix)) // Key1 version
	buf.WriteByte(key2)
	binary.Write(&buf, binary.BigEndian, uint64(version2Unix)) // Key2 version

	// Serialize Node B
	binary.Write(&buf, binary.BigEndian, uint16(len(nodeBName))) // Node B name length
	buf.WriteString(nodeBName)                                   // Node B name
	// Node B keys and versions
	buf.WriteByte(key1)
	binary.Write(&buf, binary.BigEndian, uint64(version1Unix)) // Key1 version
	buf.WriteByte(key2)
	binary.Write(&buf, binary.BigEndian, uint64(version2Unix)) // Key2 version

	// Update message length (replace placeholder)
	msgBytes := buf.Bytes()
	messageLen := len(msgBytes) - 1 // Exclude protocol version byte
	binary.BigEndian.PutUint16(msgBytes[1:3], uint16(messageLen))

	// Log the resulting message for debugging
	t.Logf("Serialized message: %x", msgBytes)

	// For readability, log the structured digest
	t.Logf("Digest: \n%s - %d: %d\n%s - %d: %d\n%s - %d: %d\n%s - %d: %d",
		nodeAName, key1, version1Unix,
		nodeAName, key2, version2Unix,
		nodeBName, key1, version1Unix,
		nodeBName, key2, version2Unix,
	)

	// De-serialise here and test

}

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

}
