package src

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"
)

const (
	DIGEST_TYPE = iota
	DELTA_TYPE
)

const (
	STRING_DV = iota
	UINT8_DV
	UINT16_DV
	UINT32_DV
	UINT64_DV
	INT_DV
	BYTE_DV
	FLOAT_DV
	TIME_DV
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

// TODO Cluster logic - should be able to pass names and pull only those deltas - then make temps and serialise

func serialiseClusterDelta(cd *clusterDelta) ([]byte, error) {
	// Metadata size
	const metadataSize = 7
	length := metadataSize

	// Calculate the required buffer size
	for name, delta := range cd.delta {
		length += 1 + len(name) // 1 byte for name length + name bytes
		length += 2             // For size of delta key values

		for _, value := range delta.keyValues {
			length += 1                // 1 byte for key
			length += 8                // 8 bytes for version (int64)
			length += 1                // 1 byte for valueType
			length += 4                // 4 bytes for value length (uint32)
			length += len(value.value) // Value size
		}
	}

	length += 2 // Adding CRLF at the end

	// Allocate buffer
	deltaBuf := make([]byte, length)

	offset := 0

	// Write metadata header
	deltaBuf[0] = byte(DELTA_TYPE) // Assuming DELTA_TYPE is defined
	offset++
	binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(length))
	offset += 4
	binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(cd.delta)))
	offset += 2

	// Serialize participants
	for name, delta := range cd.delta {
		if len(name) > 255 {
			return nil, fmt.Errorf("name length exceeds 255 bytes: %s", name)
		}

		// Write name length and name
		deltaBuf[offset] = uint8(len(name))
		offset++
		copy(deltaBuf[offset:], name)
		offset += len(name)

		// Put in size of delta for the participant
		binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(cd.delta)))
		offset += 2

		for key, value := range delta.keyValues {
			// Write key
			deltaBuf[offset] = uint8(key)
			offset++

			// Write version
			binary.BigEndian.PutUint64(deltaBuf[offset:], uint64(value.version))
			offset += 8

			// Write valueType
			deltaBuf[offset] = uint8(value.valueType)
			offset++

			// Write value length and value
			binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(len(value.value)))
			offset += 4
			copy(deltaBuf[offset:], value.value)
			offset += len(value.value)
		}
	}

	// Append CRLF
	copy(deltaBuf[offset:], CLRF) // Assuming CLRF is defined as []byte{13, 10}
	offset += 2

	// Validate buffer usage
	if offset != length {
		return nil, fmt.Errorf("buffer mismatch: calculated length %d, written offset %d", length, offset)
	}

	return deltaBuf, nil
}

func deserialiseDelta(delta []byte) (*clusterDelta, error) {

	if delta[0] != byte(DELTA_TYPE) {
		return nil, fmt.Errorf("byte array is of wrong type - %x - should be %x", delta[0], byte(DELTA_TYPE))
	}

	length := len(delta)
	metaLength := binary.BigEndian.Uint32(delta[1:5])
	log.Printf("meta length %v - actual length - %v", metaLength, length)

	if length != int(metaLength) {
		return nil, fmt.Errorf("meta length does not match desired length - %x", length)
	}

	// Use header to allocate cluster map capacity
	size := binary.BigEndian.Uint16(delta[5:7])
	log.Println("size = ", size)

	cDelta := &clusterDelta{
		make(map[string]*tmpParticipant, size),
	}

	log.Println("cluster map = ", cDelta)

	offset := 7

	// Looping through each participant
	for i := 0; i < int(size); i++ {

		nameLen := int(delta[offset])
		log.Printf("name length = %v", nameLen)

		start := offset + 1
		end := start + nameLen

		name := string(delta[start:end])

		offset += 1
		offset += nameLen

		// Need to get delta size - Uint16 - 2 bytes
		deltaSize := binary.BigEndian.Uint16(delta[offset : offset+2])

		cDelta.delta[name] = &tmpParticipant{
			make(map[int]*tmpDelta, deltaSize),
		}

		offset += 2

		for j := 0; j < int(deltaSize); j++ {

			key := int(delta[offset])
			log.Printf("key = %v", key)

			offset += 1

			d := cDelta.delta[name]
			log.Printf("delta name = %v", d.keyValues)
			d.keyValues[key] = &tmpDelta{}

			// Version
			v := binary.BigEndian.Uint64(delta[offset : offset+8])
			VersionTime := time.Unix(int64(v), 0)

			offset += 8

			// Type
			vType := int(delta[offset])
			offset += 1

			// Length
			vLength := int(binary.BigEndian.Uint32(delta[offset : offset+4]))
			offset += 4

			log.Printf("vLength = %v", vLength)

			// Value
			value := delta[offset : offset+vLength]

			log.Printf("value length = %v", len(value))

			d.keyValues[key] = &tmpDelta{
				valueType: vType,
				version:   VersionTime.Unix(),
				value:     value,
			}

			offset += vLength

		}

	}

	return cDelta, nil

}

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
