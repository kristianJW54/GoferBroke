package src

import (
	"encoding/binary"
	"fmt"
	"log"
)

const (
	CLRF = "\r\n"
)

// Version + Header Sizes
const (
	PROTO_VERSION_1 uint8 = 1
	HEADER_SIZE_V1        = 6
)

const (
	MAX_BUFF_SIZE     = 4096
	MIN_BUFF_SIZE     = 256
	INITIAL_BUFF_SIZE = 512
)

// Commands
const (
	GOSS_SYN uint8 = iota
	GOSS_SYN_ACK
	GOSS_ACK
	TEST
	ENTRY_TO_CLUSTER
	ERROR // Error message
)

// Handshake
const (
	CONNECT_REQUEST = iota
	REJECTED
	CONNECTED
)

//TODO Consider implementing interface with serialisation methods and sending mehtods for packets
// which packets can implement, then we can pass an interface to handle connection methods on server...?

//Think about versioning, if more things get added to the header - in serialisation we need to account for different versions

type PacketHeader struct {
	ProtoVersion  uint8  // Protocol version (1 byte)
	Command       uint8  // Message type (e.g., SYN, SYN_ACK, ACK..)
	MsgLength     uint16 // Total message length (4 bytes)
	PayloadLength uint16 // Total length (4 bytes)
	//May Implement Data length if we want dynamic header size for evolving protocol
}

//Type v2 header struct??

func newProtoHeader(version, command uint8) *PacketHeader {
	return &PacketHeader{
		version,
		command,
		0,
		0,
	}
}

func (pr *PacketHeader) headerV1Serialize(length uint16) ([]byte, error) {

	if pr.ProtoVersion != PROTO_VERSION_1 {
		return nil, fmt.Errorf("version must be %v", PROTO_VERSION_1)
	}

	b := make([]byte, HEADER_SIZE_V1+2+length)
	b[0] = pr.ProtoVersion
	//We will let these go until they get passed to elements that need to parse them and then let them return the error?
	b[1] = pr.Command

	binary.BigEndian.PutUint16(b[2:4], length)
	binary.BigEndian.PutUint16(b[4:6], length+HEADER_SIZE_V1)
	// Add CRLF at the end of the header
	copy(b[6:8], CLRF)

	return b, nil
}

type TCPPacket struct {
	Header *PacketHeader
	Data   []byte
}

//==========================================================
// Serialisation
//==========================================================

func (gp *TCPPacket) MarshallBinary() (data []byte, err error) {

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

func (gp *TCPPacket) marshallBinaryV1() (data []byte, err error) {

	// Calculate the length of the data (payload)
	//dataLength := uint16(len(gp.Data)) // The length of the data section

	dataLength := uint16(260)

	b, err := gp.Header.headerV1Serialize(dataLength)
	if err != nil {
		return nil, err
	}

	// Now copy the data into the byte slice after the header (start at byte 5)
	copy(b[HEADER_SIZE_V1+2:], gp.Data) // Start after header + CRLF

	return b, nil

}

//TODO Once the header is fully read, parse the message length, and then proceed to read the
// payload based on this length.

// This seems to stay after reading raw bytes from the stream

func (gp *TCPPacket) UnmarshallBinaryV1(data []byte) error {

	if data[0] != PROTO_VERSION_1 {
		return fmt.Errorf("protocol version mismatch. Expected %d, got %d", gp.Header.ProtoVersion, data[0])
	}

	// Ensure the data is at least as large as the header size (5 bytes)
	if len(data) < 10 {
		return fmt.Errorf("data too short")
	}

	// Read the header fields
	gp.Header.ProtoVersion = data[0]
	gp.Header.Command = data[1]
	gp.Header.MsgLength = binary.BigEndian.Uint16(data[2:4])
	gp.Header.PayloadLength = binary.BigEndian.Uint16(data[6:8])

	// Verify CRLF is correct
	if data[10] != '\r' || data[11] != '\n' {
		return fmt.Errorf("CRLF sequence missing or corrupted")
	}

	// Extract the payload (after the header + CRLF)
	payloadStart := HEADER_SIZE_V1 + 2
	gp.Data = make([]byte, gp.Header.MsgLength)
	copy(gp.Data, data[payloadStart:payloadStart+int(gp.Header.MsgLength)])

	// Log the parsed header fields for debugging
	log.Printf("Parsed Header: ProtoVersion=%d, Command=%d, MessageLength=%d, PayLoadLength=%d",
		gp.Header.ProtoVersion, gp.Header.Command, gp.Header.MsgLength, gp.Header.PayloadLength)

	return nil
}
