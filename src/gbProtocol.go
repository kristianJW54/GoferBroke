package src

import "encoding/binary"

const (
	PROTO_VERSION_1 uint8 = 1
)

const (
	MAX_MESSAGE_SIZE = 1024
)

const (
	GOSSIP uint8 = iota
	//PUB
	//SUB
)

const (
	SYN uint8 = iota
	SYN_ACK
	ACK
	ERROR // Error message
)

//TODO Consider implementing interface with serialisation methods and sending mehtods for packets
// which packets can implement, then we can pass an interface to handle connection methods on server...?

type protoHeader struct {
	ProtoVersion  uint8 // Protocol version (1 byte)
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
	protoHeader
	digest []synNodeMap
}

type synAckData struct {
	nodeID []byte
	delta  []Delta
}

type synAckPacket struct {
	protoHeader
	delta  []synAckData
	digest []synNodeMap
}

type GossipPayload struct {
	Header protoHeader
	Data   []byte
}

//==========================================================
// Serialisation
//==========================================================

func (gp *GossipPayload) MarshallBinary() (data []byte, err error) {

	//determine header size
	//TODO implement dynamic header size approach by specifying message size and data size
	//TODO currently using fixed header size approach

	// Calculate the length of the data (payload)
	dataLength := uint16(len(gp.Data))

	// Total message length (header + data)
	totalLength := uint16(1 + 1 + 1 + 2 + 2 + dataLength)

	b := make([]byte, 0, totalLength)

	b[0] = gp.Header.ProtoVersion
	b[1] = gp.Header.MessageType
	b[2] = gp.Header.Command

	binary.BigEndian.PutUint16(b[3:5], dataLength)
	binary.BigEndian.PutUint16(b[5:7], totalLength)

	copy(b[7:], gp.Data)

	return b, nil

}
