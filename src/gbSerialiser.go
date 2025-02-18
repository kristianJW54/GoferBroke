package src

import (
	"encoding/binary"
	"fmt"
	"time"
)

const (
	DIGEST_TYPE = iota + 1
	DELTA_TYPE
)

const (
	CONFIG_D = iota
	STATE_D
	INTERNAL_D
	CLIENT_D
)

//-------------------
//Digest for initial gossip - per connection/node - will be passed as []*clusterDigest

type digest struct {
	senderName string
	nodeName   string
	maxVersion int64
}

type fullDigest map[string]*digest

//-------------------

type tmpParticipant struct {
	keyValues map[string]*Delta
	vi        []string
}

type clusterDelta struct {
	sender string
	delta  map[string]*tmpParticipant
}

//=========================================================
// Serialise based on digest received and populate queues
//=========================================================

func (s *GBServer) serialiseSelfInfo(participant *Participant) ([]byte, error) {

	// Type = Delta - 1 byte Uint8
	// Length of payload - 4 byte uint32
	// Size of Delta - 2 byte uint16
	// Total metadata for digest byte array = --> 7 <--

	length := 7 + 2 //Including CLRF at the end

	// Include sender's name
	length += 1
	length += len(s.ServerName)

	self := participant
	selfName := s.ServerName

	// This seems like a duplication - but the same deserialise delta method will be used to deconstruct into a tmpClusterDelta
	// For that a sender name field is populated and a tmpParticipant
	length += 1 + len(self.name) + 2 // 1 byte for name length + name length + size of delta key-values

	for _, delta := range self.keyValues {
		if delta != nil {

			value := delta.value
			// Calculate the size for this delta
			length += 14 + +len(delta.key) + len(value) // 14 bytes for metadata + value length
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

	deltaBuf[offset] = uint8(len(selfName))
	offset++
	copy(deltaBuf[offset:], selfName)
	offset += len(selfName)

	// Write participant name length and name
	deltaBuf[offset] = uint8(len(selfName))
	offset++
	copy(deltaBuf[offset:], selfName)
	offset += len(selfName)

	// Write the number of key-value pairs for the participant
	binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(self.keyValues)))
	offset += 2

	for _, value := range self.keyValues {

		v := value
		key := value.key
		// Write key (which is similar to how we handle name)
		deltaBuf[offset] = uint8(len(key))
		offset++
		copy(deltaBuf[offset:], key)
		offset += len(key)

		// Write version (8 bytes, uint64)
		binary.BigEndian.PutUint64(deltaBuf[offset:], uint64(v.version))
		offset += 8

		// Write valueType (1 byte, uint8)
		deltaBuf[offset] = v.valueType
		offset++
		// Write value length (4 bytes, uint32) and the value itself
		binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(len(v.value)))
		offset += 4
		copy(deltaBuf[offset:], v.value)
		offset += len(v.value)

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

// TODO Need to serialise system critical deltas first and then fill the buffer until MTU is reached

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

	pi := s.clusterMap.participants

	//log.Printf("%s --- length of pi ========================== %v", s.ServerName, len(pi))

	for _, p := range pi {

		length += 1 + len(p.name) + 2 // 1 byte for name length + name length + size of delta key-values

		participant := p

		for key, delta := range participant.keyValues {
			if delta != nil {
				value := delta.value
				// Calculate the size for this delta
				length += 14 + len(key) + len(value) // 14 bytes for metadata + value length
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
		participant := p
		// Write participant name length and name
		deltaBuf[offset] = uint8(len(participant.name))
		offset++
		copy(deltaBuf[offset:], participant.name)
		offset += len(participant.name)

		// Write the number of key-value pairs for the participant
		binary.BigEndian.PutUint16(deltaBuf[offset:], uint16(len(participant.keyValues)))
		offset += 2

		// Serialize each key in 'keysToSerialize'
		for k, value := range participant.keyValues {

			v := value
			key := k
			// Write key (which is similar to how we handle name)
			deltaBuf[offset] = uint8(len(key))
			offset++
			copy(deltaBuf[offset:], key)
			offset += len(key)

			// Write version (8 bytes, uint64)
			binary.BigEndian.PutUint64(deltaBuf[offset:], uint64(value.version))
			offset += 8

			// Write valueType (1 byte, uint8)
			deltaBuf[offset] = v.valueType
			offset++

			// Write value length (4 bytes, uint32) and the value itself
			binary.BigEndian.PutUint32(deltaBuf[offset:], uint32(len(v.value)))
			offset += 4
			//log.Printf("%s value ----- ==================== %s", s.ServerName, v.value)
			copy(deltaBuf[offset:], v.value)
			offset += len(v.value)

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

	// Senders Name Length - 2 bytes
	// Senders Name

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	length := 7 + 2 //Including CLRF at the end

	// Include sender's name
	length += 1
	length += len(s.ServerName)

	for _, v := range cm.participants {

		length += 1
		length += len(v.name)
		length += 8 // Time unix which is int64 --> 8 Bytes

	}

	digestBuf := make([]byte, length)

	offset := 0

	digestBuf[0] = DIGEST_TYPE
	offset++
	binary.BigEndian.PutUint32(digestBuf[offset:], uint32(length))
	offset += 4
	binary.BigEndian.PutUint16(digestBuf[offset:], uint16(len(s.clusterMap.participants))) // Number of participants
	offset += 2

	digestBuf[offset] = uint8(len(s.ServerName))
	offset++
	copy(digestBuf[offset:], s.ServerName)
	offset += len(s.ServerName)

	for _, value := range s.clusterMap.participants {

		v := value

		//if len(value.name) > 255 {
		//	return nil, fmt.Errorf("name length exceeds 255 bytes: %s", value.name)
		//}
		nameLen := len(v.name)
		digestBuf[offset] = uint8(nameLen)
		offset++
		copy(digestBuf[offset:], v.name)
		offset += nameLen
		binary.BigEndian.PutUint64(digestBuf[offset:], uint64(v.maxVersion))
		offset += 8

	}
	copy(digestBuf[offset:], CLRF)
	offset += len(CLRF)

	return digestBuf, nil

}

func (s *GBServer) serialiseClusterDigestWithArray(subsetArray []string, subsetSize int) ([]byte, error) {

	// Need type = Digest - 1 byte Uint8
	// Need length of payload - 4 byte uint32
	// Need size of digest - 2 byte uint16
	// Total metadata for digest byte array = 7

	// Senders Name Length - 2 bytes
	// Senders Name

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	length := subsetSize
	length += 9

	// Include sender's name
	length += 1
	length += len(s.ServerName)

	digestBuf := make([]byte, length)

	offset := 0

	digestBuf[0] = DIGEST_TYPE
	offset++
	binary.BigEndian.PutUint32(digestBuf[offset:], uint32(length))
	offset += 4
	binary.BigEndian.PutUint16(digestBuf[offset:], uint16(3)) // Number of participants
	offset += 2

	digestBuf[offset] = uint8(len(s.ServerName))
	offset++
	copy(digestBuf[offset:], s.ServerName)
	offset += len(s.ServerName)

	for _, value := range subsetArray {

		v := cm.participants[value]

		nameLen := len(v.name)
		digestBuf[offset] = uint8(nameLen)
		offset++
		copy(digestBuf[offset:], v.name)
		offset += nameLen
		binary.BigEndian.PutUint64(digestBuf[offset:], uint64(v.maxVersion))
		offset += 8

	}
	copy(digestBuf[offset:], CLRF)
	offset += len(CLRF)

	return digestBuf, nil

}

//TODO When we deserialise into a tmp struct such as fullDigest - we need to make sure we kill the reference as soon as
// we are done with it OR find a way to compare against the raw bytes in flight to avoid over allocating memory

// This is still in the read-loop where the parser has called a handler for a specific command
// the handler has then needed to deSerialise in order to then carry out the command
// if needed, the server will be reached through the client struct which has the server embedded
func deSerialiseDigest(digestRaw []byte) (senderName string, fd *fullDigest, err error) {

	//CLRF Check
	//if bytes.HasSuffix(digest, []byte(CLRF)) {
	//	bytes.Trim(digest, CLRF)
	//}

	length := len(digestRaw)
	lengthMeta := binary.BigEndian.Uint32(digestRaw[1:5])
	//log.Println("lengthMeta = ", lengthMeta)
	if length != int(lengthMeta) {
		return "", nil, fmt.Errorf("length does not match")
	}

	sizeMeta := binary.BigEndian.Uint16(digestRaw[5:7])
	//log.Println("sizeMeta = ", sizeMeta)

	digestMap := make(fullDigest)

	offset := 7

	// Extract senders name
	senderLen := int(digestRaw[offset])
	sender := digestRaw[offset+1 : offset+1+senderLen]

	offset++
	offset += senderLen

	for i := 0; i < int(sizeMeta); i++ {
		fd := &digest{
			senderName: string(sender),
		}

		nameLen := int(digestRaw[offset])

		start := offset + 1
		end := start + nameLen
		name := string(digestRaw[start:end])
		fd.nodeName = name

		offset += 1
		offset += nameLen

		// Extract maxVersion
		maxVersion := binary.BigEndian.Uint64(digestRaw[offset : offset+8])

		// Convert maxVersion to time
		maxVersionTime := time.Unix(int64(maxVersion), 0)

		fd.maxVersion = maxVersionTime.Unix()

		digestMap[name] = fd

		offset += 8

	}

	return string(sender), &digestMap, nil
}

//=======================================================
// Serialisation for Deltas/Cluster Map
//=======================================================
