package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
)

const (
	CLRF = "\r\n"
)

//TODO -- We are still going to have MTU size BUT, the protocol will look at max delta sizes (configured) in order to balance the protocol
// There will be considerations, gossip rounds that go over MTU require TCP fragmentation so will incur network overhead - this will be on the user to balance
// Digests will have to be carefully balanced to ensure that there is enough room for large enough deltas -- if there is a large delta there should be warnings and system
// logs to track impact and encourage chunking from the user or hashing pointers etc.

const (
	MTU        = 1460 // Although we are using TCP to build a protocol on top - we should aim to fit gossip messages within MTU to avoid packet segmentation and increased network strain
	MTU_DIGEST = MTU - 460
	MTU_DELTA  = MTU - MTU_DIGEST
)

// Version + Header Sizes
const (
	PROTO_VERSION_1     uint8 = 1
	HEADER_SIZE_V1            = 6
	NODE_HEADER_SIZE_V1       = 12
)

const (
	MAX_BUFF_SIZE     = 4096
	MIN_BUFF_SIZE     = 256
	INITIAL_BUFF_SIZE = 512
)

//------------------------
//Packet constructor and serialisation

type nodePacketHeader struct {
	version    uint8
	command    uint8
	requestID  uint16
	respID     uint16
	msgSize    uint16
	headerSize uint16
}

type nodePacket struct {
	*nodePacketHeader
	data []byte
}

func constructNodeHeader(version, command uint8, reqID, respID, msgSize, headerSize uint16) *nodePacketHeader {
	return &nodePacketHeader{version, command, reqID, respID, msgSize, headerSize}
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
	//header[10] = nph.streamBatchSize
	//header[11] = nph.streamBatchSequence
	header[10] = '\r'
	header[11] = '\n'

	//log.Println("Header:", header)

	return header, nil

}

func (np *nodePacket) serialize() ([]byte, error) {

	packetCerealErr := Errors.KnownInternalErrors[Errors.PACKET_CEREAL_CODE]

	header, err := np.serializeHeader()
	if err != nil {
		return nil, packetCerealErr
	}

	dataBuf := make([]byte, np.msgSize+np.headerSize)
	copy(dataBuf, header)
	copy(dataBuf[np.headerSize:], np.data)
	return dataBuf, nil
}

func prepareRequest(data []byte, version, command int, req, resp uint16) ([]byte, error) {

	switch version {
	case 1:
		header := constructNodeHeader(1, uint8(command), req, resp, uint16(len(data)), NODE_HEADER_SIZE_V1)
		packet := &nodePacket{
			header,
			data,
		}
		payload, gbErr := packet.serialize()
		if gbErr != nil {
			return nil, fmt.Errorf("prepareRequest: serialize failed: %w", gbErr)
		}
		return payload, nil
	}

	return nil, fmt.Errorf("prepareRequest: version is not 1")

}

// Response Codes ??/

//=====================================================================
// Node Response Success
//=====================================================================

var OKRequester = []byte("1 -+- OK -+-\r\n")
var OKResponder = []byte("2 -+- OK RESP -+-\r\n")

func (c *gbClient) sendOK(reqID uint16) {

	ok, err := prepareRequest(OKRequester, 1, OK, reqID, uint16(0))
	if err != nil {
		return
	}

	c.mu.Lock()
	c.enqueueProto(ok)
	c.mu.Unlock()

}

func (c *gbClient) sendOKResp(respID uint16) {

	ok, err := prepareRequest(OKResponder, 1, OK_RESP, uint16(0), respID)
	if err != nil {
		return
	}

	c.mu.Lock()
	c.enqueueProto(ok)
	c.mu.Unlock()

}

func (c *gbClient) sendErr(reqID, respID uint16, err string) {

	errBytes := []byte(err)

	errPay, _ := prepareRequest(errBytes, 1, ERR_R, reqID, respID)

	c.mu.Lock()
	c.enqueueProto(errPay)
	c.mu.Unlock()

}

func (c *gbClient) sendErrResp(reqID, respID uint16, err string) {

	errBytes := []byte(err)

	errPay, _ := prepareRequest(errBytes, 1, ERR_RESP, reqID, respID)

	c.mu.Lock()
	c.enqueueProto(errPay)
	c.mu.Unlock()

}

// We only de-serialize when we need to produce a readable output

//=====================================================================
// Client Protocol
//=====================================================================

// Current Client Header format [V 0 0] 1st Byte = Command followed by 2 Bytes for message length

// For client id in headers we should provision for a 64 byte length id? or some form of standard id length which
// Clients can add and we echo back in our response
