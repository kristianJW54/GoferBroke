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
	PROTO_VERSION_1     uint8 = 1
	HEADER_SIZE_V1            = 6
	NODE_HEADER_SIZE_V1       = 9
)

const (
	MAX_BUFF_SIZE     = 4096
	MIN_BUFF_SIZE     = 256
	INITIAL_BUFF_SIZE = 512
)

// Node Commands
const (
	GOSS_SYN uint8 = iota
	GOSS_SYN_ACK
	GOSS_ACK
	TEST
	INITIAL
	ERROR // Error message
)

// Flags
const (
	CONNECTED = iota
	TEMP
	GOSSIPING
	GOSS_SYN_SENT
	GODD_SYN_REC
	GOSS_SYN_ACK_SENT
	GOSS_SYN_ACK_REC
	GOSS_ACK_SENT
	GOSS_ACK_REC
)

// Response Codes ??/

//TODO Consider implementing interface with serialisation methods and sending mehtods for packets
// which packets can implement, then we can pass an interface to handle connection methods on server...?

//=====================================================================
// Packet constructor and serialisation
//=====================================================================

type nodePacketHeader struct {
	version    uint8
	command    uint8
	id         uint8
	msgSize    uint16
	headerSize uint16
}

type nodePacket struct {
	*nodePacketHeader
	data []byte
}

func constructNodeHeader(version, command, id uint8, msgSize, headerSize uint16) *nodePacketHeader {
	return &nodePacketHeader{version, command, id, msgSize, headerSize}
}

func (nph *nodePacketHeader) serializeHeader() ([]byte, error) {

	if nph.version != PROTO_VERSION_1 {
		return nil, fmt.Errorf("version not supported")
	}

	if nph.headerSize != NODE_HEADER_SIZE_V1 {
		return nil, fmt.Errorf("header size not supported")
	}

	header := make([]byte, NODE_HEADER_SIZE_V1)
	header[0] = nph.version
	header[1] = nph.command
	header[2] = nph.id
	binary.BigEndian.PutUint16(header[3:5], nph.msgSize)
	binary.BigEndian.PutUint16(header[5:7], nph.headerSize)
	header[7] = '\r'
	header[8] = '\n'

	return header, nil

}

func (np *nodePacket) serialize() ([]byte, error) {

	header, err := np.serializeHeader()
	if err != nil {
		return nil, err
	}

	log.Println(header)

	dataBuf := make([]byte, np.msgSize+np.headerSize)
	copy(dataBuf, header)
	copy(dataBuf[np.headerSize:], np.data)
	return dataBuf, nil
}

// We only de-serialize when we need to produce a readable output
