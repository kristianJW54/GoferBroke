package src

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

//===================================================================================
// Cluster Map
//===================================================================================

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

// Internal Delta Keys [NOT TO BE USED EXTERNALLY]
const (
	_ADDRESS_      = "ADDR"
	_CPU_USAGE_    = "CPU_USAGE"
	_MEMORY_USAGE  = "MEMORY_USAGE"
	_NODE_CONNS_   = "NODE_CONNS"
	_CLIENT_CONNS_ = "CLIENT_CONNS"
	_HEARTBEAT_    = "HEARTBEAT"
)

type Seed struct {
	seedAddr *net.TCPAddr
}

//-------------------
// Main cluster map for gossiping

type Delta struct {
	valueType byte // Type could be internal, config, state, client
	version   int64
	value     []byte // Value should go last for easier de-serialisation
	// Could add user defined metadata later on??
}

type Participant struct {
	name       string // Possibly can remove
	keyValues  map[string]*Delta
	valueIndex []string
	maxVersion int64
	paValue    float64   // Not to be gossiped
	conn       *gbClient // Not to be gossiped
	pm         sync.RWMutex
}

type ClusterMap struct {
	seedServer   *Seed
	participants map[string]*Participant
	partIndex    []string
	phiAccMap    map[string]*phiAccrual
}

//-------------------
//Heartbeat Monitoring

type phiAccrual struct {
	threshold   int
	windowSize  int
	lastBeat    int64
	currentBeat int64
	window      map[int]int64
	pa          sync.Mutex
}

// -- Maybe a PhiAcc map with node-name as key and phiAccrual as value?

// handler for first gossip round
// command will be syn
// inside - will need to create a digest and queue it, then wait for response
// once response given - it will be syn-ack, which we will need to call the ack handler and process etc

// if syn received - need to call syn-ack handler which will generate a digest and a delta to queue, response will be an ack

//=======================================================
// Cluster Map Handling
//=======================================================

func initClusterMap(name string, seed *net.TCPAddr, participant *Participant) *ClusterMap {

	cm := &ClusterMap{
		&Seed{seedAddr: seed},
		make(map[string]*Participant),
		make([]string, 0),
		make(map[string]*phiAccrual),
	}

	// We don't add the phiAccrual here as we don't track our own internal failure detection

	cm.participants[name] = participant
	cm.partIndex = append(cm.partIndex, name)

	return cm

}

//--------
//Update cluster

//--

//Add/Remove Participant

// --
// Thread safe
func (s *GBServer) addParticipantFromTmp(name string, tmpP *tmpParticipant) error {

	s.clusterMapLock.Lock()

	// First add to the cluster map
	s.clusterMap.participants[name] = &Participant{
		name:       name,
		keyValues:  tmpP.keyValues,
		valueIndex: tmpP.vi,
	}

	log.Println("VI ----------> ", tmpP.vi)

	s.clusterMap.partIndex = append(s.clusterMap.partIndex, name)

	s.clusterMapLock.Unlock()

	return nil
}

//=======================================================
// Delta Comparisons
//=======================================================

// TODO Consider a digest pool to use to ease pressure on the Garbage Collector
// TODO We could serialise directly from the cluster map and make a byte digest - the receiver will then only have to build a tmpDigest

func (s *GBServer) generateDigest() ([]*fullDigest, error) {

	s.clusterMapLock.RLock()
	defer s.clusterMapLock.RUnlock()

	if s.clusterMap.participants == nil {
		return nil, fmt.Errorf("cluster map is empty")
	}

	td := make([]*fullDigest, len(s.clusterMap.participants))

	cm := s.clusterMap.participants

	idx := 0
	for _, value := range cm {
		// Lock the participant to safely read the data
		value.pm.RLock()
		// Initialize the map entry for each participant
		td[idx] = &fullDigest{
			nodeName:    value.name,
			maxVersion:  value.maxVersion,
			keyVersions: make(map[string]int64, len(value.keyValues)),
			vi:          value.valueIndex,
		}

		for _, v := range value.valueIndex {
			td[idx].keyVersions[v] = value.keyValues[v].version
		}

		idx++
		value.pm.RUnlock() // Release the participant lock
	}

	return td, nil
}

//--------
//Compare Digest

//=======================================================
// Delta Parsing, Handling and Changes
//=======================================================

func (s *GBServer) parseClientDelta(delta []byte, msgLen, keyLen, valueLen int) (int, error) {

	switch delta[0] {
	case 'V':

		s.clusterMapLock.Lock()
		defer s.clusterMapLock.Unlock()

		// TODO Need error checks here + correct locking

		key := delta[3 : 3+keyLen]

		value := delta[3+keyLen+1 : 2+keyLen+valueLen]

		now := time.Now().Unix()

		s.selfInfo.keyValues[string(key)] = &Delta{
			valueType: CLIENT_D,
			version:   now,
			value:     value,
		}

		s.selfInfo.valueIndex = append(s.selfInfo.valueIndex, string(key))

	}

	return 0, nil
}
