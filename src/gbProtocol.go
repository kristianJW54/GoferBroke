package src

import "net"

const (
	PROTO_VERSION_1 uint8 = 1
	HEADER_SIZE     uint8 = 6 // Adjusted below for field explanations
)

const (
	MESSAGE_TYPE uint8 = iota
	SYN
	SYN_ACK
	ACK
	ERROR // Error message
)

type protoHeader struct {
	ProtoVersion  uint8  // Protocol version (1 byte)
	MessageType   uint8  // Message type (e.g., INITIAL_PUSH_PULL)
	PeerIP        net.IP // Peer IP (4 bytes for IPv4 or 16 for IPv6 [although IPv6 can be made 4 bytes])
	PeerPort      uint16 // Peer port (2 bytes)
	MessageLength uint16 // Total message length (2 bytes)
}

type synData struct { //Maps node id to max version
	nodeID     []byte
	maxVersion uint64
}

type syn struct {
	protoHeader
	data []synData
}

type synAckData struct {
}

type synAck struct {
	protoHeader
}
