package src

import (
	"encoding/binary"
	"fmt"
)

const (
	CLRF = "\r\n"
)

const (
	MTU = 1400
)

// Version + Header Sizes
const (
	PROTO_VERSION_1     uint8 = 1
	HEADER_SIZE_V1            = 6
	NODE_HEADER_SIZE_V1       = 14
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
// Node Response Errors
//=====================================================================

var gossipError = []byte("11 -x- Gossip deferred -x-\r\n")

//=====================================================================
// Node Response Success
//=====================================================================

var respOK = []byte("1 -+- OK -+-\r\n")

//------------------------
//Packet constructor and serialisation

type nodePacketHeader struct {
	version             uint8
	command             uint8
	requestID           uint16
	respID              uint16
	msgSize             uint16
	headerSize          uint16
	streamBatchSize     uint8
	streamBatchSequence uint8
}

type nodePacket struct {
	*nodePacketHeader
	data []byte
}

func constructNodeHeader(version, command uint8, reqID, respID, msgSize, headerSize uint16, batchSize, batchSequence uint8) *nodePacketHeader {
	return &nodePacketHeader{version, command, reqID, respID, msgSize, headerSize, batchSize, batchSequence}
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
	binary.BigEndian.PutUint16(header[2:4], nph.requestID)
	binary.BigEndian.PutUint16(header[4:6], nph.respID)

	binary.BigEndian.PutUint16(header[6:8], nph.msgSize)
	binary.BigEndian.PutUint16(header[8:10], nph.headerSize)
	header[10] = nph.streamBatchSize
	header[11] = nph.streamBatchSequence
	header[12] = '\r'
	header[13] = '\n'

	//log.Println("Header:", header)

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
