package src

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// Response Codes ??/

//=====================================================================
// Errors
//=====================================================================

const (
	NETWORK_ERR_LEVEL = iota + 1
	INTERNAL_ERR_LEVEL
	SYSTEM_ERR_LEVEL
)

type GBError struct {
	Code     int
	ErrLevel int
	ErrMsg   string
	Err      error
}

// Implement `error` interface
func (e *GBError) Error() string {
	switch e.ErrLevel {
	case NETWORK_ERR_LEVEL:
		return fmt.Sprintf("%d -x- %s -x-\r\n", e.Code, e.ErrMsg)
	case INTERNAL_ERR_LEVEL:
		return fmt.Sprintf("%d: %s", e.Code, e.ErrMsg)
	case SYSTEM_ERR_LEVEL:
		if e.Err != nil {
			return fmt.Sprintf("%d: %s --> %v", e.Code, e.ErrMsg, e.Err)
		}
		return fmt.Sprintf("%d: %s", e.Code, e.ErrMsg)
	}
	return "unknown error level"
}

func (e *GBError) ToError() error {
	switch e.ErrLevel {
	case NETWORK_ERR_LEVEL:
		return fmt.Errorf("%d -x- %s -x-\r\n", e.Code, e.ErrMsg)
	case INTERNAL_ERR_LEVEL:
		return fmt.Errorf("%d: %s", e.Code, e.ErrMsg)
	case SYSTEM_ERR_LEVEL:
		if e.Err != nil {
			return fmt.Errorf("%d: %s --> %v", e.Code, e.ErrMsg, e.Err)
		}
		return fmt.Errorf("%d: %s", e.Code, e.ErrMsg)
	}
	return errors.New("unknown error level")
}

func (e *GBError) ToBytes() []byte {
	return []byte(e.Error())
}

func BytesToError(errMsg []byte) error {
	if len(errMsg) == 0 {
		return nil
	}

	strMsg := strings.TrimSpace(string(errMsg))
	var code int
	var msg string
	var errLevel int

	// Check if it's a network error format: "11 -x- Message -x-"
	if strings.Contains(strMsg, " -x- ") {
		parts := strings.SplitN(strMsg, " -x- ", 3)
		if len(parts) < 2 {
			return errors.New("invalid network error format")
		}

		c, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return errors.New("invalid network error code format")
		}

		code = c
		msg = strings.TrimSpace(parts[1])
		errLevel = NETWORK_ERR_LEVEL

	} else if strings.Contains(strMsg, ": ") { // Internal error format: "51: Message"
		parts := strings.SplitN(strMsg, ": ", 2)
		if len(parts) < 2 {
			return errors.New("invalid internal error format")
		}

		c, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return errors.New("invalid internal error code format")
		}

		code = c
		msg = strings.TrimSpace(parts[1])
		errLevel = INTERNAL_ERR_LEVEL

	} else {
		// If no match, classify it as a SYSTEM ERROR
		errLevel = SYSTEM_ERR_LEVEL
		code = -1
		msg = strMsg
	}

	switch errLevel {
	case NETWORK_ERR_LEVEL:
		if knownErr, exists := knownNetworkErrors[code]; exists {
			return knownErr
		}
	case INTERNAL_ERR_LEVEL:
		if knownErr, exists := knownInternalErrors[code]; exists {
			return knownErr
		}
	case SYSTEM_ERR_LEVEL:
		// Wrap unexpected errors
		return WrapSystemError(errors.New(msg))
	}

	// If no match, return a generic error
	return &GBError{Code: code, ErrLevel: SYSTEM_ERR_LEVEL, ErrMsg: msg}
}

func WrapSystemError(err error) *GBError {
	if err == nil {
		return nil
	}
	return &GBError{
		Code:     -1, // No predefined code for system errors
		ErrLevel: SYSTEM_ERR_LEVEL,
		ErrMsg:   "System error",
		Err:      err,
	}
}

func ErrLevelCheck(err error) string {
	var gErr *GBError
	if errors.As(err, &gErr) {
		switch gErr.ErrLevel {
		case NETWORK_ERR_LEVEL:
			return "Network Error"
		case INTERNAL_ERR_LEVEL:
			return "Internal Error"
		case SYSTEM_ERR_LEVEL:
			return "System Error"
		default:
			return "Unknown Error Type"
		}
	}
	return "Not a GBError"
}

func UnwrapToGBError(err error) *GBError {
	if err == nil {
		return nil
	}

	var gbErr *GBError
	if errors.As(err, &gbErr) {
		return gbErr
	}

	unwrapped := errors.Unwrap(err)
	if unwrapped != nil {
		return UnwrapToGBError(unwrapped)
	}
	return WrapSystemError(err)
}

type GBErrorHandlerFunc func(err error, level int)

func HandleError(gbErr *GBError, callback GBErrorHandlerFunc) error {

	return nil

}

// Network Error codes

const (
	GOSSIP_DEFERRED_CODE = 11
	GOSSIP_TIMEOUT_CODE  = 12
)

// Internal Error codes

const (
	PACKET_CEREAL_CODE = 51
	KNOWN_ADDR_CODE    = 52
)

var knownNetworkErrors = map[int]*GBError{
	GOSSIP_DEFERRED_CODE: &GBError{Code: GOSSIP_DEFERRED_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip deferred"},
	GOSSIP_TIMEOUT_CODE:  &GBError{Code: GOSSIP_TIMEOUT_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip timeout"},
}

var knownInternalErrors = map[int]*GBError{
	PACKET_CEREAL_CODE: &GBError{Code: PACKET_CEREAL_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "packet serialisation error"},
	KNOWN_ADDR_CODE:    &GBError{Code: KNOWN_ADDR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "no known addresses in internal cluster map"},
}

//=====================================================================
// Node Response Success
//=====================================================================

var OKRequester = []byte("1 -+- OK -+-\r\n")
var OKResponder = []byte("2 -+- OK RESP -+-\r\n")

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

func (np *nodePacket) serialize() ([]byte, *GBError) {

	packetCerealErr := knownInternalErrors[PACKET_CEREAL_CODE]

	header, err := np.serializeHeader()
	if err != nil {
		return nil, packetCerealErr
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
