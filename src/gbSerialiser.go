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

type fullDigest struct {
	senderName string
	nodeName   string
	maxVersion int64
}

//-------------------

type tmpParticipant struct {
	keyValues map[string]*Delta
	vi        []string
}

type clusterDelta struct {
	sender string
	delta  map[string]*tmpParticipant
}

// TODO Look at flattening or refining the data structure passed to the serialiser for faster performance

// TODO Look at efficiency - can we do this in once pass with byte.Buffer?

// TODO Cluster logic - should be able to pass names and pull only those deltas - then make temps and serialise

// TODO Should be able to pass size and skip loop step if we have it - if not, calc size ourselves in here

// TODO NEED --> A serialise self-info delta
// TODO NEED --> To add senders name into the packet at beginning for ID purposes as maps in go are not inherently ordered

func (s *GBServer) serialiseSelfInfo() ([]byte, error) {

	// Type = Delta - 1 byte Uint8
	// Length of payload - 4 byte uint32
	// Size of Delta - 2 byte uint16
	// Total metadata for digest byte array = --> 7 <--

	length := 7 + 2 //Including CLRF at the end

	// Include sender's name
	length += 1
	length += len(s.ServerName)

	self := s.selfInfo

	// This seems like a duplication - but the same deserialise delta method will be used to deconstruct into a tmpClusterDelta
	// For that a sender name field is populated and a tmpParticipant
	length += 1 + len(self.name) + 2 // 1 byte for name length + name length + size of delta key-values

	for _, delta := range self.deltaQ {
		if delta != nil {
			// Calculate the size for this delta
			length += 14 + +len(delta.key) + len(delta.value) // 14 bytes for metadata + value length
		} else {
			// Log missing delta and continue
			fmt.Printf("Warning: Delta is nil for participant %s\n", self.name)
		}
	}

	// Allocate buffer
	deltaBuf := make([]byte, length)

	// Write metadata header directly
	deltaBuf[0] = byte(DELTA_TYPE)
	binary.BigEndian.PutUint32(deltaBuf[1:5], uint32(length))
	binary.BigEndian.PutUint16(deltaBuf[5:7], uint16(1)) // As is selfInfo - will only be 1 participant

	offset := 7

	deltaBuf[offset] = uint8(len(s.ServerName))
	offset++
	copy(deltaBuf[offset:], s.ServerName)
	offset += len(s.ServerName)

	// Write participant name length and name
	deltaBuf[offset] = uint8(len(s.ServerName))
	offset++
	copy(deltaBuf[offset:], s.ServerName)
	offset += len(s.ServerName)

	// Write the number of key-value pairs for the participant
	binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(self.valueIndex)))
	offset += 2

	for _, value := range self.deltaQ {

		key := value.key
		// Write key (which is similar to how we handle name)
		deltaBuf[offset] = uint8(len(key))
		offset++
		copy(deltaBuf[offset:], key)
		offset += len(key)

		// Write version (8 bytes, uint64)
		binary.BigEndian.PutUint64(deltaBuf[offset:], uint64(value.version))
		offset += 8

		// Write valueType (1 byte, uint8)
		deltaBuf[offset] = uint8(value.valueType)
		offset++
		// Write value length (4 bytes, uint32) and the value itself
		binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(len(value.value)))
		offset += 4
		copy(deltaBuf[offset:], value.value)
		offset += len(value.value)

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

// Lock should be held on entry
func (s *GBServer) serialiseClusterDelta() ([]byte, error) {

	// Type = Delta - 1 byte Uint8
	// Length of payload - 4 byte uint32
	// Size of Delta - 2 byte uint16
	// Total metadata for digest byte array = --> 7 <--

	length := 7 + 2 //Including CLRF at the end

	// Include senders name
	length += 1
	length += len(s.ServerName)

	pi := s.clusterMap.partIndex

	for _, p := range pi {

		participant := s.clusterMap.participants[p]
		length += 1 + len(p) + 2 // 1 byte for name length + name length + size of delta key-values

		for _, delta := range participant.deltaQ {
			if delta != nil {
				// Calculate the size for this delta
				length += 14 + len(delta.key) + len(delta.value) // 14 bytes for metadata + value length
			} else {
				// Log missing delta and continue
				fmt.Printf("Warning: Delta is nil for participant %s\n", s.ServerName)
			}
		}

	}

	// Allocate buffer
	deltaBuf := make([]byte, length)

	// Write metadata header directly
	deltaBuf[0] = byte(DELTA_TYPE)
	binary.BigEndian.PutUint32(deltaBuf[1:5], uint32(length))
	binary.BigEndian.PutUint16(deltaBuf[5:7], uint16(len(pi)))

	offset := 7

	deltaBuf[offset] = uint8(len(s.ServerName))
	offset++
	copy(deltaBuf[offset:], s.ServerName)
	offset += len(s.ServerName)

	// Serialize participants
	for _, p := range pi {
		// Get the participant's data (avoiding repeated map lookups)
		participant := s.clusterMap.participants[p]
		// Write participant name length and name
		deltaBuf[offset] = uint8(len(p))
		offset++
		copy(deltaBuf[offset:], p)
		offset += len(p)

		// Write the number of key-value pairs for the participant
		binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(participant.keyValues)))
		offset += 2

		// Serialize each key in 'keysToSerialize'
		for _, value := range participant.deltaQ {

			v := value.key
			// Write key (which is similar to how we handle name)
			deltaBuf[offset] = uint8(len(v))
			offset++
			copy(deltaBuf[offset:], v)
			offset += len(v)

			// Write version (8 bytes, uint64)
			binary.BigEndian.PutUint64(deltaBuf[offset:], uint64(value.version))
			offset += 8

			// Write valueType (1 byte, uint8)
			deltaBuf[offset] = uint8(value.valueType)
			offset++

			// Write value length (4 bytes, uint32) and the value itself
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

	offset := 7

	// Extract senders name
	senderLen := int(delta[offset])
	senderName := delta[offset+1 : offset+1+senderLen]

	offset++
	offset += senderLen

	cDelta := &clusterDelta{
		string(senderName),
		make(map[string]*tmpParticipant, size),
	}

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
			make([]string, 0, deltaSize),
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

			// Add key to the vi array
			d.vi = append(d.vi, key)

			// Version
			v := binary.BigEndian.Uint64(delta[offset : offset+8])
			VersionTime := time.Unix(int64(v), 0)

			offset += 8

			// Type
			vType := delta[offset]
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

func (s *GBServer) serialiseClusterDigest() ([]byte, error) {

	// TODO Needs to have efficient allocations

	// Need type = Digest - 1 byte Uint8
	// Need length of payload - 4 byte uint32
	// Need size of digest - 2 byte uint16
	// Total metadata for digest byte array = 7

	length := 7 + 2 //Including CLRF at the end

	// Include sender's name
	length += 1
	length += len(s.ServerName)

	for _, v := range s.clusterMap.partIndex {
		value := s.clusterMap.participants[v]

		length += 1
		length += len(value.name)
		length += 8 + 2 // Time unix which is int64 --> 8 Bytes plus 2 for the number of delta key/versions in array

		for _, v := range value.valueIndex {

			length += 1
			length += len(v)
			length += 8

		}

	}

	digestBuf := make([]byte, length)

	offset := 0

	digestBuf[0] = DIGEST_TYPE
	offset++
	binary.BigEndian.PutUint32(digestBuf[offset:], uint32(length))
	offset += 4
	binary.BigEndian.PutUint16(digestBuf[offset:], uint16(len(s.clusterMap.partIndex))) // Number of participants
	offset += 2

	digestBuf[offset] = uint8(len(s.ServerName))
	offset++
	copy(digestBuf[offset:], s.ServerName)
	offset += len(s.ServerName)

	for _, v := range s.clusterMap.partIndex {

		value := s.clusterMap.participants[v]

		if len(value.name) > 255 {
			return nil, fmt.Errorf("name length exceeds 255 bytes: %s", value.name)
		}
		nameLen := len(value.name)
		digestBuf[offset] = uint8(nameLen)
		offset++
		copy(digestBuf[offset:], value.name)
		offset += nameLen
		binary.BigEndian.PutUint64(digestBuf[offset:], uint64(value.maxVersion))
		offset += 8

		binary.BigEndian.PutUint16(digestBuf[offset:], uint16(len(value.keyValues)))
		offset += 2

		for _, v := range value.valueIndex {
			version := value.keyValues[v].version
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

// Need to trust that we are being given a digest slice from the packet and message length checks have happened before
// being handed to this method
func serialiseDigest(digest []*fullDigest) ([]byte, error) {

	// TODO Needs to have efficient allocations

	// Node1: maxVersion
	// Node2: maxVersion

	// Need type = Digest - 1 byte Uint8
	// Need length of payload - 4 byte uint32
	// Need size of digest - 2 byte uint16
	// Total metadata for digest byte array = 7

	length := 7

	for _, value := range digest {
		length += 1
		length += len(value.nodeName)
		length += 8 + 2 // Time unix which is int64 --> 8 Bytes plus 2 for the number of delta versions

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

	// Extract senders name
	senderLen := int(digest[offset])
	senderName := digest[offset+1 : offset+1+senderLen]

	offset++
	offset += senderLen

	for i := 0; i < int(sizeMeta); i++ {
		fd := &fullDigest{
			senderName: string(senderName),
		}

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

	}

	return digestMap, nil
}

//=======================================================
// Serialisation for Deltas/Cluster Map
//=======================================================
