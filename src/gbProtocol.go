package src

import (
	"encoding/binary"
	"fmt"
)

const (
	CLRF = "\r\n"
)

// Version + Header Sizes
const (
	PROTO_VERSION_1     uint8 = 1
	HEADER_SIZE_V1            = 6
	NODE_HEADER_SIZE_V1       = 11
)

const (
	MAX_BUFF_SIZE     = 4096
	MIN_BUFF_SIZE     = 256
	INITIAL_BUFF_SIZE = 512
)

// Response Codes ??/

//TODO Consider implementing interface with serialisation methods and sending mehtods for packets
// which packets can implement, then we can pass an interface to handle connection methods on server...?

//=====================================================================
// Node Protocol
//=====================================================================

//------------------------
//Packet constructor and serialisation

type nodePacketHeader struct {
	version             uint8
	command             uint8
	id                  uint8
	msgSize             uint16
	headerSize          uint16
	streamBatchSize     uint8
	streamBatchSequence uint8
}

type nodePacket struct {
	*nodePacketHeader
	data []byte
}

func constructNodeHeader(version, command, id uint8, msgSize, headerSize uint16, batchSize, batchSequence uint8) *nodePacketHeader {
	return &nodePacketHeader{version, command, id, msgSize, headerSize, batchSize, batchSequence}
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
	header[7] = nph.streamBatchSize
	header[8] = nph.streamBatchSequence
	header[9] = '\r'
	header[10] = '\n'

	return header, nil

}

func (np *nodePacket) serialize() ([]byte, error) {

	header, err := np.serializeHeader()
	if err != nil {
		return nil, err
	}

	dataBuf := make([]byte, np.msgSize+np.headerSize)
	copy(dataBuf, header)
	copy(dataBuf[np.headerSize:], np.data)
	return dataBuf, nil
}

// We only de-serialize when we need to produce a readable output

//=====================================================================
// Client Protocol
//=====================================================================

// Current Client Header format [V 0 0] 1st Byte = Command followed by 2 Bytes for message length

// For client id in headers we should provision for a 64 byte length id? or some form of standard id length which
// Clients can add and we echo back in our response
