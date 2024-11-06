package src

import "net"

//===================================================================================
// Cluster Map
//===================================================================================

type Seed struct {
	seedAddr *net.TCPAddr
}

type Delta struct {
	key     string
	value   string
	version int64
}

type Participant struct {
	name       string
	deltas     map[string][]Delta
	maxVersion int64
}

type ClusterMap struct {
	seedServer   Seed
	participants map[string]Participant
}
