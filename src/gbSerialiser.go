package src

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

const (
	DIGEST_TYPE = iota
	DELTA_TYPE
)

const (
	CONFIG_D = iota
	STATE_D
	INTERNAL_D
	CLIENT_D
)

const (
	CerealPoolSmall  = 512
	CerealPoolMedium = 1024
	CerealPoolLarge  = 2048
)

var cerealPoolSmall = &sync.Pool{
	New: func() any {
		b := [CerealPoolSmall]byte{}
		return &b
	},
}

var cerealPoolMedium = &sync.Pool{
	New: func() any {
		b := [CerealPoolMedium]byte{}
		return &b
	},
}

var cerealPoolLarge = &sync.Pool{
	New: func() any {
		b := [CerealPoolLarge]byte{}
		return &b
	},
}

func cerealPoolGet(size int) []byte {
	var buf []byte
	switch {
	case size <= CerealPoolSmall:
		//log.Printf("Acquiring small node pool")
		buf = cerealPoolSmall.Get().(*[CerealPoolSmall]byte)[:size] // Slice to the correct size
	case size <= CerealPoolMedium:
		//log.Printf("Acquiring medium node pool")
		buf = cerealPoolMedium.Get().(*[CerealPoolMedium]byte)[:size] // Slice to the correct size
	default:
		//log.Printf("Acquiring large node pool")
		buf = cerealPoolLarge.Get().(*[CerealPoolLarge]byte)[:size] // Slice to the correct size
	}
	return buf
}

func cerealPoolPut(b []byte) {
	switch cap(b) {
	case CerealPoolSmall:
		//log.Printf("returning small buffer to pool - %v", len(b))
		b := (*[CerealPoolSmall]byte)(b[0:CerealPoolSmall])
		nbPoolSmall.Put(b)
	case CerealPoolMedium:
		b := (*[CerealPoolMedium]byte)(b[0:CerealPoolMedium])
		nbPoolMedium.Put(b)
	case CerealPoolLarge:
		b := (*[CerealPoolLarge]byte)(b[0:CerealPoolLarge])
		nbPoolLarge.Put(b)
	default:

	}
}

//-------------------
//Digest for initial gossip - per connection/node - will be passed as []*clusterDigest

type summaryDigest struct {
	name       string
	maxVersion int64
}

type fullDigest struct {
	nodeName    string
	maxVersion  int64
	keyVersions map[string]int64
	vi          []string
}

//-------------------

type tmpParticipant struct {
	keyValues map[string]*Delta
	vi        []string
}

type clusterDelta struct {
	delta map[string]*tmpParticipant
}

// TODO Look at flattening or refining the data structure passed to the serialiser for faster performance

// TODO Look at efficiency - can we do this in once pass with byte.Buffer?

// TODO Cluster logic - should be able to pass names and pull only those deltas - then make temps and serialise

// TODO Should be able to pass size and skip loop step if we have it - if not, calc size ourselves in here

func serialiseClusterDelta(cd *clusterDelta, pi []string) ([]byte, error) {

	length := 7 + 2 // Including CLRF

	// Pre-calculate buffer size (with minimal overhead)
	for _, idx := range pi {
		participant := cd.delta[idx]
		length += 1 + len(idx) + 2 // 1 byte for name length + name length + size of delta key-values

		for _, value := range participant.vi {
			key := value
			valueData := participant.keyValues[value]
			length += 14 + len(key) + len(valueData.value) // 1 byte for key length + key length + 8 bytes for version + 1 byte for valueType + 4 bytes for value length
		}
	}

	// Allocate buffer
	deltaBuf := make([]byte, length)
	//deltaBuf := cerealPoolGet(length)

	// Write metadata header directly
	deltaBuf[0] = byte(DELTA_TYPE)
	binary.BigEndian.PutUint32(deltaBuf[1:5], uint32(length))
	binary.BigEndian.PutUint16(deltaBuf[5:7], uint16(len(cd.delta)))

	offset := 7

	//// Serialize participants

	// Efficient serialization of clusterDelta participants
	for _, idx := range pi {
		// Get the participant's data (avoiding repeated map lookups)
		participant := cd.delta[idx]

		// Write participant name length and name
		deltaBuf[offset] = uint8(len(idx))
		offset++
		copy(deltaBuf[offset:], idx)
		offset += len(idx)

		// Write the number of key-value pairs for the participant
		binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(participant.keyValues)))
		offset += 2

		// Serialize each key-value pair for the participant
		for _, value := range participant.vi {
			// Retrieve the key-value pair from the participant's keyValues map
			valueData := participant.keyValues[value]
			v := value

			// Write key (which is similar to how we handle name)
			deltaBuf[offset] = uint8(len(v))
			offset++
			copy(deltaBuf[offset:], v)
			offset += len(v)

			// Write version (8 bytes, uint64)
			binary.BigEndian.PutUint64(deltaBuf[offset:], uint64(valueData.version))
			offset += 8

			// Write valueType (1 byte, uint8)
			deltaBuf[offset] = uint8(valueData.valueType)
			offset++

			// Write value length (4 bytes, uint32) and the value itself
			binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(len(valueData.value)))
			offset += 4
			copy(deltaBuf[offset:], valueData.value)
			offset += len(valueData.value)
		}
	}

	// Append CRLF
	copy(deltaBuf[offset:], CLRF) // Assuming CLRF is defined as []byte{13, 10}
	offset += 2

	// Validate buffer usage
	if offset != length {
		return nil, fmt.Errorf("buffer mismatch: calculated length %d, written offset %d", length, offset)
	}

	//defer cerealPoolPut(deltaBuf)

	return deltaBuf, nil
}

// TODO Better error handling to pass up
func deserialiseDelta(delta []byte) (*clusterDelta, error) {

	if delta[0] != byte(DELTA_TYPE) {
		return nil, fmt.Errorf("byte array is of wrong type - %x - should be %x", delta[0], byte(DELTA_TYPE))
	}

	length := len(delta)
	metaLength := binary.BigEndian.Uint32(delta[1:5])

	if length != int(metaLength) {
		return nil, fmt.Errorf("meta length does not match desired length - %x", length)
	}

	// Use header to allocate cluster map capacity
	size := binary.BigEndian.Uint16(delta[5:7])

	cDelta := &clusterDelta{
		make(map[string]*tmpParticipant, size),
	}

	offset := 7

	// Looping through each participant
	for i := 0; i < int(size); i++ {

		nameLen := int(delta[offset])

		start := offset + 1
		end := start + nameLen

		name := string(delta[start:end])

		offset += 1
		offset += nameLen

		// Need to get delta size - Uint16 - 2 bytes
		deltaSize := binary.BigEndian.Uint16(delta[offset : offset+2])

		cDelta.delta[name] = &tmpParticipant{
			make(map[string]*Delta, deltaSize),
			nil,
		}

		offset += 2

		for j := 0; j < int(deltaSize); j++ {

			keyLen := int(delta[offset])

			start := offset + 1
			end := start + keyLen

			key := string(delta[start:end])

			offset += 1
			offset += keyLen

			d := cDelta.delta[name]
			d.keyValues[key] = &Delta{}

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

			// Value
			value := delta[offset : offset+vLength]

			d.keyValues[key] = &Delta{
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
func serialiseDigest(digest []*fullDigest) ([]byte, error) {

	// TODO Needs to have efficient allocations
	//node1: 10 [user123: 5, user456: 8, user789: 4, ]
	//node2: 12 [user123: 6, user456: 10, user789: 7, ]
	//node3: 9 [user123: 5, user456: 9, user789: 6, ]

	// Need type = Digest - 1 byte Uint8
	// Need length of payload - 4 byte uint32
	// Need size of digest - 2 byte uint16
	// Total metadata for digest byte array = 7

	length := 7

	for _, value := range digest {
		length += 1
		length += len(value.nodeName)
		length += 8 + 2 // Time unix which is int64 --> 8 Bytes plus 2 for the number of delta versions

		for _, v := range value.vi {
			length += 1
			length += len(v)
			length += 8
		}
	}

	length += 2 // Adding CLRF at the end

	digestBuf := make([]byte, length)

	offset := 0

	digestBuf[0] = DIGEST_TYPE
	offset++
	binary.BigEndian.PutUint32(digestBuf[offset:], uint32(length))
	offset += 4
	binary.BigEndian.PutUint16(digestBuf[offset:], uint16(len(digest))) // Number of participants
	offset += 2
	for _, value := range digest {
		if len(value.nodeName) > 255 {
			return nil, fmt.Errorf("name length exceeds 255 bytes: %s", value.nodeName)
		}
		nameLen := len(value.nodeName)
		digestBuf[offset] = uint8(nameLen)
		offset++
		copy(digestBuf[offset:], value.nodeName)
		offset += nameLen
		binary.BigEndian.PutUint64(digestBuf[offset:], uint64(value.maxVersion))
		offset += 8

		binary.BigEndian.PutUint16(digestBuf[offset:], uint16(len(value.keyVersions)))
		offset += 2

		for _, v := range value.vi {
			version := value.keyVersions[v]
			keyLen := len(v)
			digestBuf[offset] = uint8(keyLen)
			offset++
			copy(digestBuf[offset:], v)
			offset += keyLen
			binary.BigEndian.PutUint64(digestBuf[offset:], uint64(version))
			offset += 8
		}

	}
	copy(digestBuf[offset:], CLRF)
	offset += len(CLRF)

	return digestBuf, nil
}

// This is still in the read-loop where the parser has called a handler for a specific command
// the handler has then needed to deSerialise in order to then carry out the command
// if needed, the server will be reached through the client struct which has the server embedded
func deSerialiseDigest(digest []byte) ([]*fullDigest, error) {

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

	digestMap := make([]*fullDigest, sizeMeta)

	offset := 7

	for i := 0; i < int(sizeMeta); i++ {
		fd := &fullDigest{}

		nameLen := int(digest[offset])

		start := offset + 1
		end := start + nameLen
		name := string(digest[start:end])
		fd.nodeName = name

		offset += 1
		offset += nameLen

		// Extract maxVersion
		maxVersion := binary.BigEndian.Uint64(digest[offset : offset+8])

		// Convert maxVersion to time
		maxVersionTime := time.Unix(int64(maxVersion), 0)

		fd.maxVersion = maxVersionTime.Unix()

		digestMap[i] = fd

		offset += 8

		deltaSize := binary.BigEndian.Uint16(digest[offset : offset+2])

		fd.keyVersions = make(map[string]int64, deltaSize)

		offset += 2

		for j := 0; j < int(deltaSize); j++ {

			keyLen := int(digest[offset])
			start := offset + 1
			end := start + keyLen
			key := string(digest[start:end])

			offset += keyLen + 1

			//Extract key version
			version := int64(binary.BigEndian.Uint64(digest[offset : offset+8]))

			versionTime := time.Unix(int64(version), 0)

			offset += 8

			fd.keyVersions[key] = versionTime.Unix()

		}

	}

	return digestMap, nil
}

//=======================================================
// Serialisation for Deltas/Cluster Map
//=======================================================
