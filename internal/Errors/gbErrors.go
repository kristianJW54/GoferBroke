package Errors

import (
	"errors"
	"fmt"
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
	if e == nil {
		return "<nil>"
	}

	base := fmt.Sprintf("[%s] %d: %s -x", e.getErrLevelTag(), e.Code, e.ErrMsg)

	// Recursively unwrap any nested GBErrors
	if e.Err != nil {
		return base + " " + e.Err.Error()
	}

	return base
}

func (e *GBError) trimmedError() string {
	// Like Error(), but omits \r\n for nested NETWORK_ERR_LEVEL
	return fmt.Sprintf("[%s] %d: %s -x", e.getErrLevelTag(), e.Code, e.ErrMsg)
}

// ChainGBErrorf creates a new GBError using a base GBError as template,
// applies formatted message, and wraps the given cause (can be nil).
func ChainGBErrorf(template *GBError, cause error, format string, args ...any) *GBError {

	// Build the new ErrMsg:
	var msg string
	if format != "" {
		msg = fmt.Sprintf("%s — %s", template.ErrMsg, fmt.Sprintf(format, args...))
	} else {
		msg = template.ErrMsg
	}

	return &GBError{
		Code:     template.Code,
		ErrLevel: template.ErrLevel,
		ErrMsg:   msg,
		Err:      unwrapToGBErrorFromFmt(cause),
	}
}

// TODO we should try to preserve the formatted string message as well

func (e *GBError) Net() string {
	return e.Error() + "\r\n"
}

func unwrapToGBErrorFromFmt(err error) error {
	for err != nil {
		var GBError *GBError
		if errors.As(err, &GBError) {
			return err
		}
		err = errors.Unwrap(err)
	}
	return nil
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

// TODO This needs fixing

func BytesToError(errMsg []byte) error {
	if len(errMsg) == 0 {
		return nil
	}

	// Convert and trim input
	strMsg := strings.TrimSpace(string(errMsg))

	// Extract all GBError strings from message
	gbStrings := ExtractGBErrors(errors.New(strMsg))
	if len(gbStrings) == 0 {
		// If not structured, fallback to wrapping raw
		return WrapSystemError(errors.New(strMsg))
	}

	// Unwrap all GBErrors
	gbErrs := UnwrapGBErrors(gbStrings)
	if len(gbErrs) == 0 {
		return WrapSystemError(errors.New(strMsg)) // parsing failed
	}

	// Return the most recent / deepest GBError
	return gbErrs[len(gbErrs)-1]
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

	var clone GBError

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
			clone = *knownErr
			clone.ErrMsg = msg
			return &clone, nil
		}
	case INTERNAL_ERR_LEVEL:
		if knownErr, exists := KnownInternalErrors[code]; exists {
			clone = *knownErr
			clone.ErrMsg = msg
			return &clone, nil
		}
	case SYSTEM_ERR_LEVEL:
		// Wrap unexpected errors
		return WrapSystemError(errors.New(msg)), nil
	}

	// If no match, return a generic error
	return &GBError{Code: code, ErrLevel: SYSTEM_ERR_LEVEL, ErrMsg: msg}, nil

}

type GBErrorHandlerFunc func(gbErr *GBError)

func HandleError(err error, callback func(gbErrors []*GBError) error) error {
	if err == nil {
		return nil
	}

	// Extract structured GBErrors from the formatted string
	rawErrors := ExtractGBErrors(err)
	if len(rawErrors) == 0 {
		// Nothing to unwrap — treat as a system-level fallback
		return err
	}

	gbErrors := UnwrapGBErrors(rawErrors)

	// Let caller pick one to return or inspect
	if callback != nil {
		if cbErr := callback(gbErrors); cbErr != nil {
			return cbErr
		}
	}

	// Default: return the most severe or last one
	return nil
}

func RecoverFromPanic() error {
	if r := recover(); r != nil {
		// Convert the recovered panic into a structured GBError
		sysErr := WrapSystemError(fmt.Errorf("panic recovered: %v", r))
		fmt.Println("Recovered from panic:", sysErr) // Optional logging
		return sysErr
	}
	return nil
}

// Network Error codes

const (
	GOSSIP_DEFERRED_CODE              = 11
	GOSSIP_TIMEOUT_CODE               = 12
	DESERIALISE_TYPE_CODE             = 13
	DESERIALISE_LENGTH_CODE           = 14
	CONDUCTING_DISCOVERY_CODE         = 15
	NO_DIGEST_CODE                    = 16
	DESERIALISE_DIGEST_CODE           = 17
	NIL_DIGEST_CODE                   = 18
	NAME_NOT_FOUND_IN_FULLDIGEST_CODE = 19
	CONFIG_CHECKSUM_FAIL_CODE         = 20
	CONFIG_AVAILABLE_CODE             = 21
	PROBE_FAILED_CODE                 = 22
	INVALID_CHECKSUM_CODE             = 23
)

// Internal Error codes

const (
	PACKET_CEREAL_CODE                = 51
	KNOWN_ADDR_CODE                   = 52
	INVALID_ERROR_FORMAT              = 53
	INVALID_ERROR_CODE                = 54
	NO_REQUEST_ID_CODE                = 55
	DISCOVERY_REQUEST_CODE            = 56
	RESPONSE_CODE                     = 57
	ADDR_MAP_CODE                     = 58
	EMPTY_ADDR_MAP_CODE               = 59
	ADD_DICSOVERY_CODE                = 60
	EMPTY_PARTICIPANT_HEAP_CODE       = 61
	GOSS_ACK_CODE                     = 62
	NODE_NOT_FOUND_CODE               = 63
	CLUSTER_CONFIG_CODE               = 64
	DELTA_UPDATE_NO_DELTA_CODE        = 65
	DELTA_UPDATE_KEY_CODE             = 66
	ADD_GSA_DELTA_CODE                = 67
	UNABLE_TO_DISCOVER_ADVERTISE_CODE = 68
	DIAL_SEED_CODE                    = 69
	INTERNAL_ERROR_HANDLER_CODE       = 70
	CONNECT_TO_SEED_CODE              = 71
	GET_CONFIG_DELTAS_FOR_RECON_CODE  = 72
	SERIALISE_DELTA_CODE              = 73
	ADDING_CONFIG_UPDATE_SELF         = 74
	WANT_CONFIG_GROUP                 = 75
	CONFIG_DIGEST_CODE                = 76
	RANDOM_SEED_CODE                  = 77
	GET_NODE_CONN_CODE                = 78
	RESOLVE_SEED_ADDR_CODE            = 79
	DECODE_DELTA_CODE                 = 80
	CONSTRUCT_NODE_HEADER_CODE        = 81
	PREPARE_REQUEST_CODE              = 82
	INDIRECT_PROBE_CODE               = 83
	MARK_SUSPECT_CODE                 = 84
	CHECK_FAILURE_GSA_CODE            = 85
	UPDATE_SELF_INFO_CODE             = 86
	SEND_CONFIG_CHECKSUM_CODE         = 87
	SEND_CONFIG_DELTA_CODE            = 88
	CONNECT_TO_NODE_CODE              = 89
	PREPARE_SELF_INFO_CODE            = 90
	SEED_SEND_SELF_CODE               = 91
	RANDOM_NODE_INDEXES_CODE          = 92
	CONFIG_CHECKSUM_RESP_CODE         = 93
)

var KnownNetworkErrors = map[int]*GBError{
	GOSSIP_DEFERRED_CODE:              &GBError{Code: GOSSIP_DEFERRED_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip deferred"},
	GOSSIP_TIMEOUT_CODE:               &GBError{Code: GOSSIP_TIMEOUT_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "Gossip timeout"},
	INVALID_ERROR_FORMAT:              &GBError{Code: INVALID_ERROR_FORMAT, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "invalid format"},
	INVALID_ERROR_CODE:                &GBError{Code: INVALID_ERROR_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "invalid error code"},
	DESERIALISE_TYPE_CODE:             &GBError{Code: DESERIALISE_TYPE_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "wrong type received by deserialise"},
	DESERIALISE_LENGTH_CODE:           &GBError{Code: DESERIALISE_LENGTH_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "mismatch in data length received by deserialise"},
	EMPTY_ADDR_MAP_CODE:               &GBError{Code: EMPTY_ADDR_MAP_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "address map is empty"},
	CONDUCTING_DISCOVERY_CODE:         &GBError{Code: CONDUCTING_DISCOVERY_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "conducting discovery"},
	NO_DIGEST_CODE:                    &GBError{Code: NO_DIGEST_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "no digest to send"},
	DESERIALISE_DIGEST_CODE:           &GBError{Code: DESERIALISE_DIGEST_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "failed to deserialise digest"},
	NIL_DIGEST_CODE:                   &GBError{Code: NIL_DIGEST_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "full digest returned is a nil reference/pointer"},
	NAME_NOT_FOUND_IN_FULLDIGEST_CODE: &GBError{Code: NAME_NOT_FOUND_IN_FULLDIGEST_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "full digest map entry not found"},
	CONFIG_CHECKSUM_FAIL_CODE:         &GBError{Code: CONFIG_CHECKSUM_FAIL_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "config checksum does not match"},
	CONFIG_AVAILABLE_CODE:             &GBError{Code: CONFIG_AVAILABLE_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "config checksum mismatch new config available"},
	PROBE_FAILED_CODE:                 &GBError{Code: PROBE_FAILED_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "probe failed"},
	INVALID_CHECKSUM_CODE:             &GBError{Code: INVALID_CHECKSUM_CODE, ErrLevel: NETWORK_ERR_LEVEL, ErrMsg: "invalid checksum length"},
}

var KnownInternalErrors = map[int]*GBError{
	PACKET_CEREAL_CODE:                &GBError{Code: PACKET_CEREAL_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "packet serialisation error"},
	KNOWN_ADDR_CODE:                   &GBError{Code: KNOWN_ADDR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "no known addresses in internal cluster map"},
	INVALID_ERROR_FORMAT:              &GBError{Code: INVALID_ERROR_FORMAT, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "invalid format"},
	INVALID_ERROR_CODE:                &GBError{Code: INVALID_ERROR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "invalid error code"},
	NO_REQUEST_ID_CODE:                &GBError{Code: NO_REQUEST_ID_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "no request id found"},
	DISCOVERY_REQUEST_CODE:            &GBError{Code: DISCOVERY_REQUEST_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "discovery request error"},
	RESPONSE_CODE:                     &GBError{Code: RESPONSE_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "response channel error"},
	ADDR_MAP_CODE:                     &GBError{Code: ADDR_MAP_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "address map build error"},
	EMPTY_ADDR_MAP_CODE:               &GBError{Code: EMPTY_ADDR_MAP_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "address map is empty"},
	ADD_DICSOVERY_CODE:                &GBError{Code: ADD_DICSOVERY_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "add discovery failed"},
	EMPTY_PARTICIPANT_HEAP_CODE:       &GBError{Code: EMPTY_PARTICIPANT_HEAP_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "empty participant heap"},
	GOSS_ACK_CODE:                     &GBError{Code: GOSS_ACK_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "goss ack error"},
	NODE_NOT_FOUND_CODE:               &GBError{Code: NODE_NOT_FOUND_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "node not found in cluster map"},
	CLUSTER_CONFIG_CODE:               &GBError{Code: CLUSTER_CONFIG_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "seed config error"},
	DELTA_UPDATE_NO_DELTA_CODE:        &GBError{Code: DELTA_UPDATE_NO_DELTA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "delta update - no delta found"},
	DELTA_UPDATE_KEY_CODE:             &GBError{Code: DELTA_UPDATE_KEY_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "delta update - keyGroup or Key cannot be changed"},
	ADD_GSA_DELTA_CODE:                &GBError{Code: ADD_GSA_DELTA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "error adding GSA delta to map"},
	UNABLE_TO_DISCOVER_ADVERTISE_CODE: &GBError{Code: UNABLE_TO_DISCOVER_ADVERTISE_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "unable to determine advertise address"},
	DIAL_SEED_CODE:                    &GBError{Code: DIAL_SEED_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "dial seed failed"},
	INTERNAL_ERROR_HANDLER_CODE:       &GBError{Code: INTERNAL_ERROR_HANDLER_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "internal error handler error"},
	CONNECT_TO_SEED_CODE:              &GBError{Code: CONNECT_TO_SEED_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "connecting to seed error"},
	GET_CONFIG_DELTAS_FOR_RECON_CODE:  &GBError{Code: GET_CONFIG_DELTAS_FOR_RECON_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "error finding config deltas above version"},
	SERIALISE_DELTA_CODE:              &GBError{Code: SERIALISE_DELTA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "serialise delta failed"},
	ADDING_CONFIG_UPDATE_SELF:         &GBError{Code: ADDING_CONFIG_UPDATE_SELF, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to update own config"},
	WANT_CONFIG_GROUP:                 &GBError{Code: WANT_CONFIG_GROUP, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "should receive a config group delta"},
	CONFIG_DIGEST_CODE:                &GBError{Code: CONFIG_DIGEST_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "sending config digest failed"},
	RANDOM_SEED_CODE:                  &GBError{Code: RANDOM_SEED_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to generate random seed"},
	GET_NODE_CONN_CODE:                &GBError{Code: GET_NODE_CONN_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to get node from nodeConnStore"},
	RESOLVE_SEED_ADDR_CODE:            &GBError{Code: RESOLVE_SEED_ADDR_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to resolve seed address"},
	DECODE_DELTA_CODE:                 &GBError{Code: DECODE_DELTA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to decode delta"},
	CONSTRUCT_NODE_HEADER_CODE:        &GBError{Code: CONSTRUCT_NODE_HEADER_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to construct node header"},
	PREPARE_REQUEST_CODE:              &GBError{Code: PREPARE_REQUEST_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to prepare request"},
	INDIRECT_PROBE_CODE:               &GBError{Code: INDIRECT_PROBE_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to send indirect probe"},
	MARK_SUSPECT_CODE:                 &GBError{Code: MARK_SUSPECT_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "mark suspect failed"},
	CHECK_FAILURE_GSA_CODE:            &GBError{Code: CHECK_FAILURE_GSA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "check failure during GSA received error"},
	UPDATE_SELF_INFO_CODE:             &GBError{Code: UPDATE_SELF_INFO_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to update self info"},
	SEND_CONFIG_CHECKSUM_CODE:         &GBError{Code: SEND_CONFIG_CHECKSUM_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "send config checksum failed"},
	SEND_CONFIG_DELTA_CODE:            &GBError{Code: SEND_CONFIG_DELTA_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to send cluster config delta"},
	CONNECT_TO_NODE_CODE:              &GBError{Code: CONNECT_TO_NODE_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "connect to node in map failed"},
	PREPARE_SELF_INFO_CODE:            &GBError{Code: PREPARE_SELF_INFO_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to prepare self info"},
	SEED_SEND_SELF_CODE:               &GBError{Code: SEED_SEND_SELF_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "seed send self info failed"},
	RANDOM_NODE_INDEXES_CODE:          &GBError{Code: RANDOM_NODE_INDEXES_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "failed to generate random node indexes"},
	CONFIG_CHECKSUM_RESP_CODE:         &GBError{Code: CONFIG_CHECKSUM_RESP_CODE, ErrLevel: INTERNAL_ERR_LEVEL, ErrMsg: "processing config checksum response failed"},
}

var (
	DiscoveryReqErr           = KnownInternalErrors[DISCOVERY_REQUEST_CODE]
	ResponseErr               = KnownInternalErrors[RESPONSE_CODE]
	EmptyAddrMapErr           = KnownInternalErrors[EMPTY_ADDR_MAP_CODE]
	AddDiscoveryErr           = KnownInternalErrors[ADD_DICSOVERY_CODE]
	EmptyParticipantHeapErr   = KnownInternalErrors[EMPTY_PARTICIPANT_HEAP_CODE]
	GossAckErr                = KnownInternalErrors[GOSS_ACK_CODE]
	NodeNotFoundErr           = KnownInternalErrors[NODE_NOT_FOUND_CODE]
	ClusterConfigErr          = KnownInternalErrors[CLUSTER_CONFIG_CODE]
	DeltaUpdateNoDeltaErr     = KnownInternalErrors[DELTA_UPDATE_NO_DELTA_CODE]
	DeltaUpdateKeyErr         = KnownInternalErrors[DELTA_UPDATE_KEY_CODE]
	AddGSAErr                 = KnownInternalErrors[ADD_GSA_DELTA_CODE]
	UnableAdvertiseErr        = KnownInternalErrors[UNABLE_TO_DISCOVER_ADVERTISE_CODE]
	DialSeedErr               = KnownInternalErrors[DIAL_SEED_CODE]
	InternalErrorHandlerErr   = KnownInternalErrors[INTERNAL_ERROR_HANDLER_CODE]
	ConnectSeedErr            = KnownInternalErrors[CONNECT_TO_SEED_CODE]
	ConfigDeltaVersionErr     = KnownInternalErrors[GET_CONFIG_DELTAS_FOR_RECON_CODE]
	SerialiseDeltaErr         = KnownInternalErrors[SERIALISE_DELTA_CODE]
	NoRequestIDErr            = KnownInternalErrors[NO_REQUEST_ID_CODE]
	SelfConfigUpdateErr       = KnownInternalErrors[ADDING_CONFIG_UPDATE_SELF]
	ConfigGroupErr            = KnownInternalErrors[WANT_CONFIG_GROUP]
	ConfigDigestErr           = KnownInternalErrors[CONFIG_DIGEST_CODE]
	RandomSeedErr             = KnownInternalErrors[RANDOM_SEED_CODE]
	NodeConnStoreErr          = KnownInternalErrors[GET_NODE_CONN_CODE]
	ResolveSeedAddrErr        = KnownInternalErrors[RESOLVE_SEED_ADDR_CODE]
	DecodeDeltaErr            = KnownInternalErrors[DECODE_DELTA_CODE]
	ConstructHeaderErr        = KnownInternalErrors[CONSTRUCT_NODE_HEADER_CODE]
	PrepareRequestErr         = KnownInternalErrors[PREPARE_REQUEST_CODE]
	IndirectProbeErr          = KnownInternalErrors[INDIRECT_PROBE_CODE]
	MarkSuspectErr            = KnownInternalErrors[MARK_SUSPECT_CODE]
	CheckFailureGSAErr        = KnownInternalErrors[CHECK_FAILURE_GSA_CODE]
	UpdateSelfInfoErr         = KnownInternalErrors[UPDATE_SELF_INFO_CODE]
	SendConfigChecksumErr     = KnownInternalErrors[SEND_CONFIG_CHECKSUM_CODE]
	SendClusterConfigDeltaErr = KnownInternalErrors[SEND_CONFIG_DELTA_CODE]
	ConnectToNodeErr          = KnownInternalErrors[CONNECT_TO_NODE_CODE]
	PrepareSelfInfoErr        = KnownInternalErrors[PREPARE_SELF_INFO_CODE]
	KnownAddrErr              = KnownInternalErrors[KNOWN_ADDR_CODE]
	SeedSendSelfErr           = KnownInternalErrors[SEED_SEND_SELF_CODE]
	RandomIndexesErr          = KnownInternalErrors[RANDOM_NODE_INDEXES_CODE]
	ConfigCheckSumRespErr     = KnownInternalErrors[CONFIG_CHECKSUM_RESP_CODE]
)

var (
	DeserialiseTypeErr     = KnownNetworkErrors[DESERIALISE_TYPE_CODE]
	DeserialiseLengthErr   = KnownNetworkErrors[DESERIALISE_LENGTH_CODE]
	AddrMapErr             = KnownNetworkErrors[ADDR_MAP_CODE]
	GossipDeferredErr      = KnownNetworkErrors[GOSSIP_DEFERRED_CODE]
	EmptyAddrMapNetworkErr = KnownNetworkErrors[EMPTY_ADDR_MAP_CODE]
	ConductingDiscoveryErr = KnownNetworkErrors[CONDUCTING_DISCOVERY_CODE]
	NoDigestErr            = KnownNetworkErrors[NO_DIGEST_CODE]
	DeserialiseDigestErr   = KnownNetworkErrors[DESERIALISE_DIGEST_CODE]
	NilDigestErr           = KnownNetworkErrors[NIL_DIGEST_CODE]
	FullDigestMapEntryErr  = KnownNetworkErrors[NAME_NOT_FOUND_IN_FULLDIGEST_CODE]
	ConfigChecksumFailErr  = KnownNetworkErrors[CONFIG_CHECKSUM_FAIL_CODE]
	ConfigAvailableErr     = KnownNetworkErrors[CONFIG_AVAILABLE_CODE]
	ProbeFailedErr         = KnownNetworkErrors[PROBE_FAILED_CODE]
	InvalidChecksumErr     = KnownNetworkErrors[INVALID_CHECKSUM_CODE]
)
