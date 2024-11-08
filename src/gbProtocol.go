package src

import (
	"encoding/binary"
	"fmt"
	"log"
)

const (
	PROTO_VERSION_1 uint8 = 1
	HEADER_SIZE           = 6
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

type ProtoHeader struct {
	ProtoVersion  uint8 // Protocol version (1 byte)
	ClientType    uint8
	MessageType   uint8 // Message type (e.g., SYN, SYN_ACK, ACK..)
	Command       uint8
	MessageLength uint16 // Total message length (2 bytes)
	//May Implement Data length if we want dynamic header size for evolving protocol
}

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
	Header ProtoHeader
	Data   []byte
}

//==========================================================
// Serialisation
//==========================================================

func (gp *TCPPayload) MarshallBinary() (data []byte, err error) {

	//determine header size
	//TODO implement dynamic header size approach by specifying message size and data size
	//TODO currently using fixed header size approach

	// Calculate the length of the data (payload)
	dataLength := uint16(len(gp.Data)) // The length of the data section

	totalLength := uint16(HEADER_SIZE + dataLength) // 5 bytes for the header (ProtoVersion, MessageType, Command, MessageLength) + data length

	// Create a byte slice with enough space for the header + data
	b := make([]byte, totalLength)

	// Fill the header fields
	b[0] = gp.Header.ProtoVersion
	b[1] = gp.Header.ClientType
	b[2] = gp.Header.MessageType
	b[3] = gp.Header.Command

	// Write the message length (length of the data) into the byte slice
	binary.BigEndian.PutUint16(b[4:6], dataLength) // MessageLength

	// Now copy the data into the byte slice after the header (start at byte 5)
	copy(b[6:], gp.Data)

	return b, nil

}

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
