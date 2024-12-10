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

// These functions will be methods on the client and will be use parsed packets stored in the state machines struct which is
// embedded in the client struct

// Need to trust that we are being given a digest slice from the packet and message length checks have happened before
// being handed to this method
func serialiseDigest(digest []*clusterDigest) ([]byte, error) {

	// TODO Need to understand how we are dealing with CLRF
	// TODO Think about locks and where they are being held -- if they are needed etc

	// Need type = Digest - 1 byte Uint8
	// Need length of payload - 4 byte uint32
	// Need size of digest - 2 byte uint16
	// Total metadata for digest byte array = 7

	size := 7

	for _, value := range digest {
		size += 1
		size += len(value.name)
		size += 8 // Time unix which is int64 --> 8 Bytes
	}

	size += 2 // Adding CLRF at the end

	digestBuf := make([]byte, size)

	offset := 0

	digestBuf[0] = DIGEST_TYPE
	offset++
	binary.BigEndian.PutUint32(digestBuf[offset:], uint32(size))
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
