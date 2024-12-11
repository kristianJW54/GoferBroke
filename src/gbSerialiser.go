package src

import (
	"encoding/binary"
	"fmt"
	"time"
)

const (
	DIGEST_TYPE = iota
	DELTA_TYPE
)

//-------------------
//Digest for initial gossip - per connection/node - will be passed as []*clusterDigest

type clusterDigest struct {
	name       string
	maxVersion int64
}

//-------------------
//Temp delta for use over the network separating from the internal cluster map

type tmpDelta struct {
	valueType int
	version   int64
	value     []byte // Value should go last for easier de-serialisation
}

type tmpParticipant struct {
	keyValues map[int]*tmpDelta
}

type clusterDelta struct {
	delta map[string]*tmpParticipant
}

// TODO Look at efficiency - can we do this in once pass with byte.Buffer?
// TODO Look at sync.Pool for buffer use

func serialiseClusterDelta(cd *clusterDelta) ([]byte, error) {

	//Need type = Delta - 1 byte Uint8
	//Need length of payload - 4 byte Uint32
	//Need size of delta - 2 byte Uint16
	// Total metadata for digest byte array = 7

	length := 7

	for name, delta := range cd.delta {
		length += len(name) // Need the name length

		// Loop through the delta
		for _, value := range delta.keyValues {
			length += 1 // Adding the key which is an iota 1 byte
			length += 8 // For unix version int64 - 8 byte
			length += 1 // For value type
			length += len(value.value)
		}

	}
	length += 2 // Adding CLRF at the end

	deltaBuf := make([]byte, length)

	offset := 0

	// Construct meta header
	deltaBuf[0] = byte(DELTA_TYPE)
	offset++
	binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(length))
	offset += 4
	binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(cd.delta)))
	offset += 2

	//Construct payload + meta payload details
	for name, delta := range cd.delta {
		if len(name) > 255 {
			return nil, fmt.Errorf("name length exceeds 255 bytes: %s", name)
		}

		deltaBuf[offset] = uint8(len(name))
		offset += 1
		copy(deltaBuf[offset:], name)
		offset += len(name)

		for key, value := range delta.keyValues {

			deltaBuf[offset] = uint8(key)
			offset++
			binary.BigEndian.PutUint64(deltaBuf[offset:], uint64(value.version))
			offset += 8
			deltaBuf[offset] = uint8(value.valueType)
			offset++
			binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(len(value.value)))
			offset += 4
			copy(deltaBuf[offset:], value.value)
			offset += len(value.value)

		}
	}

	copy(deltaBuf[offset:], CLRF)
	offset += len(CLRF)

	return []byte{}, nil

}

// TODO Cluster logic - should be able to pass names and pull only those deltas - then make temps and serialise

// These functions will be methods on the client and will be use parsed packets stored in the state machines struct which is
// embedded in the client struct

// Need to trust that we are being given a digest slice from the packet and message length checks have happened before
// being handed to this method
func serialiseDigest(digest []*clusterDigest) ([]byte, error) {

	// Need type = Digest - 1 byte Uint8
	// Need length of payload - 4 byte uint32
	// Need size of digest - 2 byte uint16
	// Total metadata for digest byte array = 7

	length := 7

	for _, value := range digest {
		length += 1
		length += len(value.name)
		length += 8 // Time unix which is int64 --> 8 Bytes
	}

	length += 2 // Adding CLRF at the end

	digestBuf := make([]byte, length)

	offset := 0

	digestBuf[0] = DIGEST_TYPE
	offset++
	binary.BigEndian.PutUint32(digestBuf[offset:], uint32(length))
	offset += 4
	binary.BigEndian.PutUint16(digestBuf[offset:], uint16(len(digest)))
	offset += 2
	for _, value := range digest {
		if len(value.name) > 255 {
			return nil, fmt.Errorf("name length exceeds 255 bytes: %s", value.name)
		}
		digestBuf[offset] = uint8(len(value.name))
		offset++
		copy(digestBuf[offset:], value.name)
		offset += len(value.name)
		binary.BigEndian.PutUint64(digestBuf[offset:], uint64(value.maxVersion))
		offset += 8
	}
	copy(digestBuf[offset:], CLRF)
	offset += len(CLRF)

	return digestBuf, nil
}

// This is still in the read-loop where the parser has called a handler for a specific command
// the handler has then needed to deSerialise in order to then carry out the command
// if needed, the server will be reached through the client struct which has the server embedded
func deSerialiseDigest(digest []byte) ([]*clusterDigest, error) {

	//CLRF Check
	//if bytes.HasSuffix(digest, []byte(CLRF)) {
	//	bytes.Trim(digest, CLRF)
	//}

	length := len(digest)
	lengthMeta := binary.BigEndian.Uint32(digest[1:5])
	//log.Println("lengthMeta = ", lengthMeta)
	if length != int(lengthMeta) {
		return nil, fmt.Errorf("length does not match")
	}

	sizeMeta := binary.BigEndian.Uint16(digest[5:7])
	//log.Println("sizeMeta = ", sizeMeta)

	digestMap := make([]*clusterDigest, sizeMeta)

	offset := 7

	for i := 0; i < int(sizeMeta); i++ {
		td := &clusterDigest{}

		nameLen := int(digest[offset])

		start := offset + 1
		end := start + nameLen
		name := string(digest[start:end])
		td.name = name

		offset += 1
		offset += nameLen

		// Extract maxVersion
		maxVersion := binary.BigEndian.Uint64(digest[offset : offset+8])

		// Convert maxVersion to time
		maxVersionTime := time.Unix(int64(maxVersion), 0)

		td.maxVersion = maxVersionTime.Unix()

		digestMap[i] = td

		offset += 8

	}

	return digestMap, nil
}

//=======================================================
// Serialisation for Deltas/Cluster Map
//=======================================================
