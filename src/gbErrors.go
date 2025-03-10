package src

import (
	"errors"
	"fmt"
	"log"
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
	baseError := fmt.Sprintf("[%s] %d: %s -x", e.getErrLevelTag(), e.Code, e.ErrMsg)
	if e.Err != nil {
		return fmt.Sprintf("%s %v", baseError, e.Err) // Append wrapped error
	}
	return baseError
}

func WrapGBError(wrapperErr *GBError, err error) *GBError {
	return &GBError{
		Code:     wrapperErr.Code,
		ErrLevel: wrapperErr.ErrLevel,
		ErrMsg:   wrapperErr.ErrMsg,
		Err:      err,
	}
}

func UnwrapGBErrors(err error) []*GBError {
	var gbErrors []*GBError

	strErr := err.Error()
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

	return gbErrors
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
		if knownErr, exists := knownNetworkErrors[code]; exists {
			return knownErr, nil
		}
	case INTERNAL_ERR_LEVEL:
		if knownErr, exists := knownInternalErrors[code]; exists {
			return knownErr, nil
		}
	case SYSTEM_ERR_LEVEL:
		// Wrap unexpected errors
		return WrapSystemError(errors.New(msg)), nil
	}

	// If no match, return a generic error
	return &GBError{Code: code, ErrLevel: SYSTEM_ERR_LEVEL, ErrMsg: msg}, nil

}

func UnwrapError(err error) []*GBError {
	if err == nil {
		return nil
	}

	var gbErrors []*GBError

	var gbErr *GBError

	if errors.As(err, &gbErr) {
		gbErrors = append(gbErrors, gbErr)
		err = gbErr.Err
	}

	parsedGBErr, parseErr := ParseGBError(err.Error())
	if parseErr == nil && parsedGBErr != nil {
		gbErrors = append(gbErrors, parsedGBErr)
		err = parsedGBErr.Err
	}

	unwrapped := errors.Unwrap(err)

	if unwrapped != nil {
		moreGBErrors := UnwrapError(unwrapped)
		gbErrors = append(gbErrors, moreGBErrors...)
	}

	gbErrorsFromString := UnwrapGBErrors(err)
	for _, gbErr := range gbErrorsFromString {
		gbErrors = append(gbErrors, gbErr)
	}

	if len(gbErrors) == 0 {
		systemErr := WrapSystemError(err)
		gbErrors = append(gbErrors, systemErr)
	}

	return gbErrors
}

type GBErrorHandlerFunc func(gbErr *GBError)

func HandleError(err error, callback func(gbError []*GBError)) {

	if err == nil {
		return
	}

	gbErr := UnwrapError(err)

	callback(gbErr)

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
	GOSSIP_DEFERRED_CODE = 11
	GOSSIP_TIMEOUT_CODE  = 12
)

// Internal Error codes

const (
	PACKET_CEREAL_CODE     = 51
	KNOWN_ADDR_CODE        = 52
	INVALID_ERROR_FORMAT   = 53
	INVALID_ERROR_CODE     = 54
	NO_REQUEST_ID_CODE     = 55
	DISCOVERY_REQUEST_CODE = 56
	RESPONSE_CODE          = 57
)

var knownNetworkErrors = map[int]*GBError{
	GOSSIP_DEFERRED_CODE: &GBError{Code: GOSSIP_DEFERRED_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip deferred"},
	GOSSIP_TIMEOUT_CODE:  &GBError{Code: GOSSIP_TIMEOUT_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip timeout"},
	INVALID_ERROR_FORMAT: &GBError{Code: INVALID_ERROR_FORMAT, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "invalid format"},
	INVALID_ERROR_CODE:   &GBError{Code: INVALID_ERROR_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "invalid error code"},
}

var knownInternalErrors = map[int]*GBError{
	PACKET_CEREAL_CODE:     &GBError{Code: PACKET_CEREAL_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "packet serialisation error"},
	KNOWN_ADDR_CODE:        &GBError{Code: KNOWN_ADDR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "no known addresses in internal cluster map"},
	INVALID_ERROR_FORMAT:   &GBError{Code: INVALID_ERROR_FORMAT, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "invalid format"},
	INVALID_ERROR_CODE:     &GBError{Code: INVALID_ERROR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "invalid error code"},
	NO_REQUEST_ID_CODE:     &GBError{Code: NO_REQUEST_ID_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "no request id found"},
	DISCOVERY_REQUEST_CODE: &GBError{Code: DISCOVERY_REQUEST_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "discovery request error"},
	RESPONSE_CODE:          &GBError{Code: RESPONSE_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "response channel error"},
}

var (
	GossipDeferredErr = knownNetworkErrors[GOSSIP_DEFERRED_CODE]
	NoRequestIDErr    = knownNetworkErrors[NO_REQUEST_ID_CODE]
	DiscoveryReqErr   = knownInternalErrors[DISCOVERY_REQUEST_CODE]
	ResponseErr       = knownInternalErrors[RESPONSE_CODE]
)
