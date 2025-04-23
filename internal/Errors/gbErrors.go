package Errors

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

//=====================================================================
// Errors
//=====================================================================

const (
	NETWORK_ERR_LEVEL = iota + 1
	INTERNAL_ERR_LEVEL
	SYSTEM_ERR_LEVEL
)

var gbErrorPattern = regexp.MustCompile(`\[[A-Z]\] \d+: .*? -x`)

type GBError struct {
	Code     int
	ErrLevel int
	ErrMsg   string
	Err      error
}

func (e *GBError) getErrLevelTag() string {
	switch e.ErrLevel {
	case NETWORK_ERR_LEVEL:
		return "N"
	case INTERNAL_ERR_LEVEL:
		return "I"
	case SYSTEM_ERR_LEVEL:
		return "S"
	default:
		return "U" // Unknown
	}
}

// Implement `error` interface
func (e *GBError) Error() string {
	switch e.ErrLevel {
	case NETWORK_ERR_LEVEL:
		return fmt.Sprintf("[%s] %d: %s -x\r\n", e.getErrLevelTag(), e.Code, e.ErrMsg)
	case INTERNAL_ERR_LEVEL:
		return fmt.Sprintf("[%s] %d: %s -x", e.getErrLevelTag(), e.Code, e.ErrMsg)
	case SYSTEM_ERR_LEVEL:
		return fmt.Sprintf("[%s] %d: %s -x", e.getErrLevelTag(), e.Code, e.ErrMsg)
	}
	return fmt.Sprintf("[%s] %d: %s -x", e.getErrLevelTag(), e.Code, e.ErrMsg)
}

func WrapGBError(wrapperErr error, err error) error {
	return fmt.Errorf("%w %w", wrapperErr, err)
}

func UnwrapGBErrors(errStr []string) []*GBError {
	var gbErrors []*GBError

	for _, strErr := range errStr {
		strings.TrimSpace(strErr)

		parts := strings.Split(strErr, " -x")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			parsedErr, parseErr := ParseGBError(part)
			if parseErr == nil {
				gbErrors = append(gbErrors, parsedErr)
			}

		}
	}

	return gbErrors
}

func ExtractGBErrors(err error) []string {
	if err == nil {
		return nil
	}

	errStr := err.Error()
	matches := gbErrorPattern.FindAllString(errStr, -1)

	//log.Printf("Extracted GBErrors: %v", matches)

	return matches
}

func (e *GBError) ToError() error {
	switch e.ErrLevel {
	case NETWORK_ERR_LEVEL:
		return fmt.Errorf("[N] %d: %s -x\r\n", e.Code, e.ErrMsg)
	case INTERNAL_ERR_LEVEL:
		return fmt.Errorf("[I] %d: %s -x", e.Code, e.ErrMsg)
	case SYSTEM_ERR_LEVEL:
		if e.Err != nil {
			return fmt.Errorf("[S] %d: %s -x%v", e.Code, e.ErrMsg, e.Err)
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
	if strings.Contains(strMsg, "[N]") {
		parts := strings.SplitN(strMsg, ": ", 2)
		if len(parts) < 2 {
			return errors.New("invalid network error format")
		}

		c, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(parts[0], "[N]")))
		if err != nil {
			return errors.New("invalid network error code format")
		}

		code = c
		msg = strings.TrimSpace(parts[1])
		errLevel = NETWORK_ERR_LEVEL

	} else if strings.Contains(strMsg, "[I]") { // Internal error format: "51: Message"
		parts := strings.SplitN(strMsg, ": ", 2)
		if len(parts) < 2 {
			return errors.New("invalid internal error format")
		}

		c, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(parts[0], "[I]")))
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
		if knownErr, exists := KnownNetworkErrors[code]; exists {
			return knownErr
		}
	case INTERNAL_ERR_LEVEL:
		if knownErr, exists := KnownInternalErrors[code]; exists {
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

func ParseGBError(err string) (*GBError, error) {

	if len(err) == 0 {
		return nil, nil
	}

	strErr := err
	strMsg := strings.TrimSpace(string(strErr))
	var code int
	var msg string
	var errLevel int

	// Check if it's a network error format: "11 -x- Message -x-"
	if strings.Contains(strMsg, "[N]") {
		parts := strings.SplitN(strMsg, ": ", 2)
		if len(parts) < 2 {
			return nil, errors.New("invalid network error format")
		}

		c, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(parts[0], "[N]")))
		if err != nil {
			return nil, errors.New("invalid network error code format")
		}

		code = c
		msg = strings.TrimSpace(parts[1])
		errLevel = NETWORK_ERR_LEVEL

	} else if strings.Contains(strMsg, "[I]") { // Internal error format: "51: Message"
		parts := strings.SplitN(strMsg, ": ", 2)
		if len(parts) < 2 {
			return nil, errors.New("invalid internal error format")
		}

		c, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(parts[0], "[I]")))
		if err != nil {
			return nil, errors.New("invalid internal error code format")
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
		if knownErr, exists := KnownNetworkErrors[code]; exists {
			return knownErr, nil
		}
	case INTERNAL_ERR_LEVEL:
		if knownErr, exists := KnownInternalErrors[code]; exists {
			return knownErr, nil
		}
	case SYSTEM_ERR_LEVEL:
		// Wrap unexpected errors
		return WrapSystemError(errors.New(msg)), nil
	}

	// If no match, return a generic error
	return &GBError{Code: code, ErrLevel: SYSTEM_ERR_LEVEL, ErrMsg: msg}, nil

}

type GBErrorHandlerFunc func(gbErr *GBError)

func HandleError(err error, callback func(gbError []*GBError) error) error {

	if err == nil {
		return nil
	}

	errStr := ExtractGBErrors(err)

	gbErrs := UnwrapGBErrors(errStr)

	callBackErr := callback(gbErrs)
	if callBackErr != nil {
		return callBackErr
	}
	return err
}

func RecoverFromPanic() error {
	if r := recover(); r != nil {
		// Convert the recovered panic into a structured GBError
		sysErr := WrapSystemError(fmt.Errorf("panic recovered: %v", r))
		log.Println("Recovered from panic:", sysErr) // Optional logging
		return sysErr
	}
	return nil
}

// Network Error codes

const (
	GOSSIP_DEFERRED_CODE      = 11
	GOSSIP_TIMEOUT_CODE       = 12
	DESERIALISE_TYPE_CODE     = 13
	DESERIALISE_LENGTH_CODE   = 14
	CONDUCTING_DISCOVERY_CODE = 15
	NO_DIGEST_CODE            = 16
)

// Internal Error codes

const (
	PACKET_CEREAL_CODE          = 51
	KNOWN_ADDR_CODE             = 52
	INVALID_ERROR_FORMAT        = 53
	INVALID_ERROR_CODE          = 54
	NO_REQUEST_ID_CODE          = 55
	DISCOVERY_REQUEST_CODE      = 56
	RESPONSE_CODE               = 57
	ADDR_MAP_CODE               = 58
	EMPTY_ADDR_MAP_CODE         = 59
	ADD_DICSOVERY_CODE          = 60
	EMPTY_PARTICIPANT_HEAP_CODE = 61
	GOSS_ACK_CODE               = 62
	NODE_NOT_FOUND_CODE         = 63
	CLUSTER_CONFIG_CODE         = 64
	DELTA_UPDATE_NO_DELTA_CODE  = 65
	DELTA_UPDATE_KEY_CODE       = 66
	ADD_GSA_DELTA_CODE          = 67
)

var KnownNetworkErrors = map[int]*GBError{
	GOSSIP_DEFERRED_CODE:      &GBError{Code: GOSSIP_DEFERRED_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip deferred"},
	GOSSIP_TIMEOUT_CODE:       &GBError{Code: GOSSIP_TIMEOUT_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip timeout"},
	INVALID_ERROR_FORMAT:      &GBError{Code: INVALID_ERROR_FORMAT, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "invalid format"},
	INVALID_ERROR_CODE:        &GBError{Code: INVALID_ERROR_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "invalid error code"},
	DESERIALISE_TYPE_CODE:     &GBError{Code: DESERIALISE_TYPE_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "wrong type received by deserialise"},
	DESERIALISE_LENGTH_CODE:   &GBError{Code: DESERIALISE_LENGTH_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "mismatch in data length received by deserialise"},
	EMPTY_ADDR_MAP_CODE:       &GBError{Code: EMPTY_ADDR_MAP_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "address map is empty"},
	CONDUCTING_DISCOVERY_CODE: &GBError{Code: CONDUCTING_DISCOVERY_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "conducting discovery"},
	NO_DIGEST_CODE:            &GBError{Code: NO_DIGEST_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "no digest to send"},
}

var KnownInternalErrors = map[int]*GBError{
	PACKET_CEREAL_CODE:          &GBError{Code: PACKET_CEREAL_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "packet serialisation error"},
	KNOWN_ADDR_CODE:             &GBError{Code: KNOWN_ADDR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "no known addresses in internal cluster map"},
	INVALID_ERROR_FORMAT:        &GBError{Code: INVALID_ERROR_FORMAT, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "invalid format"},
	INVALID_ERROR_CODE:          &GBError{Code: INVALID_ERROR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "invalid error code"},
	NO_REQUEST_ID_CODE:          &GBError{Code: NO_REQUEST_ID_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "no request id found"},
	DISCOVERY_REQUEST_CODE:      &GBError{Code: DISCOVERY_REQUEST_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "discovery request error"},
	RESPONSE_CODE:               &GBError{Code: RESPONSE_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "response channel error"},
	ADDR_MAP_CODE:               &GBError{Code: ADDR_MAP_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "address map build error"},
	EMPTY_ADDR_MAP_CODE:         &GBError{Code: EMPTY_ADDR_MAP_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "address map is empty"},
	ADD_DICSOVERY_CODE:          &GBError{Code: ADD_DICSOVERY_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "add discovery failed"},
	EMPTY_PARTICIPANT_HEAP_CODE: &GBError{Code: EMPTY_PARTICIPANT_HEAP_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "empty participant heap"},
	GOSS_ACK_CODE:               &GBError{Code: GOSS_ACK_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "goss ack error"},
	NODE_NOT_FOUND_CODE:         &GBError{Code: NODE_NOT_FOUND_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "node not found in cluster map"},
	CLUSTER_CONFIG_CODE:         &GBError{Code: CLUSTER_CONFIG_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "seed config error"},
	DELTA_UPDATE_NO_DELTA_CODE:  &GBError{Code: DELTA_UPDATE_NO_DELTA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "delta update - no delta found"},
	DELTA_UPDATE_KEY_CODE:       &GBError{Code: DELTA_UPDATE_KEY_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "delta update - keyGroup or Key cannot be changed"},
	ADD_GSA_DELTA_CODE:          &GBError{Code: ADD_GSA_DELTA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "error adding GSA delta to map"},
}

var (
	DiscoveryReqErr         = KnownInternalErrors[DISCOVERY_REQUEST_CODE]
	ResponseErr             = KnownInternalErrors[RESPONSE_CODE]
	EmptyAddrMapErr         = KnownInternalErrors[EMPTY_ADDR_MAP_CODE]
	AddDiscoveryErr         = KnownInternalErrors[ADD_DICSOVERY_CODE]
	EmptyParticipantHeapErr = KnownInternalErrors[EMPTY_PARTICIPANT_HEAP_CODE]
	GossAckErr              = KnownInternalErrors[GOSS_ACK_CODE]
	NodeNotFoundErr         = KnownInternalErrors[NODE_NOT_FOUND_CODE]
	ClusterConfigErr        = KnownInternalErrors[CLUSTER_CONFIG_CODE]
	DeltaUpdateNoDeltaErr   = KnownInternalErrors[DELTA_UPDATE_NO_DELTA_CODE]
	DeltaUpdateKeyErr       = KnownInternalErrors[DELTA_UPDATE_KEY_CODE]
	AddGSAErr               = KnownInternalErrors[ADD_GSA_DELTA_CODE]
)

var (
	DeserialiseTypeErr     = KnownNetworkErrors[DESERIALISE_TYPE_CODE]
	DeserialiseLengthErr   = KnownNetworkErrors[DESERIALISE_LENGTH_CODE]
	AddrMapErr             = KnownNetworkErrors[ADDR_MAP_CODE]
	GossipDeferredErr      = KnownNetworkErrors[GOSSIP_DEFERRED_CODE]
	NoRequestIDErr         = KnownNetworkErrors[NO_REQUEST_ID_CODE]
	EmptyAddrMapNetworkErr = KnownNetworkErrors[EMPTY_ADDR_MAP_CODE]
	ConductingDiscoveryErr = KnownNetworkErrors[CONDUCTING_DISCOVERY_CODE]
	NoDigestErr            = KnownNetworkErrors[NO_DIGEST_CODE]
)
