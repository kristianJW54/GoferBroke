package src

import (
	"net"
)

//===================================================================================
// Cluster Map
//===================================================================================

const (
	STRING_DV = iota
	UINT8_DV
	UINT16_DV
	UINT32_DV
	UINT64_DV
	INT_DV
	BYTE_DV
	FLOAT_DV
	TIME_DV
)

const (
	HEARTBEAT_V = iota
	ADDR_V
	CPU_USAGE_V
	MEMORY_USAGE_V
	NUM_NODE_CONN_V
	NUM_CLIENT_CONN_V
	INTEREST_V
	ROUTES_V
)

type Seed struct {
	seedAddr *net.TCPAddr
}

type Delta struct {
	valueType int
	version   int64
	value     []byte // Value should go last for easier de-serialisation
}

type Participant struct {
	name       string
	keyValues  map[int]*Delta
	maxVersion int64
}

type ClusterMap struct {
	seedServer   Seed
	participants map[string]*Participant
}

// TODO Think about functions or methods which will take new participant data and serialise/de-serialise it for adding to map
// TODO Think about where these functions need to live and how to handle
