package src

import (
	"encoding/binary"
	"fmt"
	"log"
)

const (
	PROTO_VERSION_1 uint8 = 1
	HEADER_SIZE_V1        = 6
)

const (
	MAX_MESSAGE_SIZE  = 1024
	INITIAL_BUFF_SIZE = 512
)

const (
	NODE uint8 = iota
	CLIENT
)

const (
	GOSSIP uint8 = iota
	NEW_NODE
	//PUB
	//SUB
)

const (
	SYN uint8 = iota
	SYN_ACK
	ACK
	TEST
	ENTRY_TO_CLUSTER
	ERROR // Error message
)

//TODO Consider implementing interface with serialisation methods and sending mehtods for packets
// which packets can implement, then we can pass an interface to handle connection methods on server...?

//Think about versioning, if more things get added to the header - in serialisation we need to account for different versions

type ProtoHeader struct {
	ProtoVersion  uint8 // Protocol version (1 byte)
	ClientType    uint8
	MessageType   uint8 // Message type (e.g., SYN, SYN_ACK, ACK..)
	Command       uint8
	MessageLength uint16 // Total message length (2 bytes)
	//May Implement Data length if we want dynamic header size for evolving protocol
}

//Type v2 header struct??

func newProtoHeader(version, clientType, messageType, command uint8) *ProtoHeader {
	return &ProtoHeader{
		version,
		clientType,
		messageType,
		command,
		0,
	}
}

func (pr *ProtoHeader) headerV1Serialize(length uint16) ([]byte, error) {

	if pr.ProtoVersion != PROTO_VERSION_1 {
		return nil, fmt.Errorf("version must be %v", PROTO_VERSION_1)
	}

	b := make([]byte, HEADER_SIZE_V1+length)
	b[0] = pr.ProtoVersion
	//We will let these go until they get passed to elements that need to parse them and then let them return the error?
	b[1] = pr.ClientType
	b[2] = pr.MessageType
	b[3] = pr.Command
	return b, nil
}

func (pr *ProtoHeader) headerV1Deserialize() (data []byte, err error) {

	return
}

//TODO Think about adding protocol specific serialisation independent of client data
//TODO Think about header serialisation...? adding to data after...? easier to get length?

type synNodeMap struct { //Maps node id to max version
	nodeID     []byte
	maxVersion uint64
}

type synPacket struct {
	ProtoHeader
	digest []synNodeMap
}

type synAckData struct {
	nodeID []byte
	delta  []Delta
}

type synAckPacket struct {
	ProtoHeader
	delta  []synAckData
	digest []synNodeMap
}

type TCPPayload struct {
	Header *ProtoHeader
	Data   []byte
}

//==========================================================
// Serialisation
//==========================================================

//TODO At the moment we have one type to serialise - if we get more then think about an interface

func (gp *TCPPayload) MarshallBinary() (data []byte, err error) {

	//TODO - we have header serialise - maybe also make data serialise by looking at header message type and length
	// and pass it to the correct serializer based on version also

	switch gp.Header.ProtoVersion {
	case PROTO_VERSION_1:
		data, err := gp.marshallBinaryV1()
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	return nil, fmt.Errorf("protocol version not supported")
}

func (gp *TCPPayload) marshallBinaryV1() (data []byte, err error) {

	// Calculate the length of the data (payload)
	dataLength := uint16(len(gp.Data)) // The length of the data section

	b, err := gp.Header.headerV1Serialize(dataLength)
	if err != nil {
		return nil, err
	}

	// Write the message length (length of the data) into the byte slice
	binary.BigEndian.PutUint16(b[4:6], dataLength) // MessageLength

	// Now copy the data into the byte slice after the header (start at byte 5)
	copy(b[6:], gp.Data)

	return b, nil

}

//TODO Once the header is fully read, parse the message length, and then proceed to read the
// payload based on this length.

// TODO Need to look at how parsing is impacted by this

func (gp *TCPPayload) UnmarshallBinaryV1(data []byte) error {

	if data[0] != PROTO_VERSION_1 {
		return fmt.Errorf("protocol version mismatch. Expected %d, got %d", gp.Header.ProtoVersion, data[0])
	}

	// Ensure the data is at least as large as the header size (5 bytes)
	if len(data) < 6 {
		return fmt.Errorf("data too short")
	}

	// Read the header fields
	gp.Header.ProtoVersion = data[0]
	gp.Header.ClientType = data[1]
	gp.Header.MessageType = data[2]
	gp.Header.Command = data[3]
	gp.Header.MessageLength = binary.BigEndian.Uint16(data[4:6])

	// Extract the data (after the header, starting at byte 5)
	gp.Data = make([]byte, gp.Header.MessageLength)
	copy(gp.Data, data[6:6+gp.Header.MessageLength])

	// Log the parsed header fields for debugging
	log.Printf("Parsed Header: ProtoVersion=%d, ClientType=%d, MessageType=%d, Command=%d, MessageLength=%d",
		gp.Header.ProtoVersion, gp.Header.ClientType, gp.Header.MessageType, gp.Header.Command, gp.Header.MessageLength)

	return nil
}
