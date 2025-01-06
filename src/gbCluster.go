package src

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

//===================================================================================
// Gossip
//===================================================================================

type gossip struct {
	gossInterval         time.Duration
	nodeSelection        uint8
	gossipControlChannel chan bool
	gossipOK             bool
	gossSignal           *sync.Cond
	gossMu               sync.Mutex
}

func initGossipSettings(gossipInterval time.Duration, nodeSelection uint8) *gossip {

	// TODO Will need to carefully incorporate with config or flags

	goss := &gossip{
		gossInterval:         gossipInterval,
		nodeSelection:        nodeSelection,
		gossipControlChannel: make(chan bool, 1),
		gossipOK:             false,
	}

	goss.gossSignal = sync.NewCond(&goss.gossMu)

	return goss
}

//=======================================================
// Gossip Signalling + Process
//=======================================================

//TODO to avoid bouncing gossip between two nodes - server should flag what node it is gossiping with
// If it selects a node to gossip with and it is the one it is currently gossiping with then it should pick another or wait

//----------------
//Gossip Signalling

func (s *GBServer) checkGossipCondition() {
	nodes := atomic.LoadInt64(&s.numNodeConnections)

	if nodes >= 1 && !s.flags.isSet(GOSSIP_SIGNALLED) {
		s.flags.set(GOSSIP_SIGNALLED)
		s.gossip.gossipControlChannel <- true
		log.Println("signalling gossip")
		s.gossip.gossSignal.Broadcast()

	} else if nodes < 1 && s.flags.isSet(GOSSIP_SIGNALLED) {
		s.gossip.gossipControlChannel <- false
		s.flags.clear(GOSSIP_SIGNALLED)
	}

}

func (s *GBServer) gossipProcess() {
	stopCondition := context.AfterFunc(s.serverContext, func() {
		// Notify all waiting goroutines to proceed if needed.
		s.gossip.gossSignal.L.Lock()
		defer s.gossip.gossSignal.L.Unlock()
		s.gossip.gossSignal.Broadcast()
	})
	defer stopCondition()

	for {
		s.gossip.gossMu.Lock()

		// Wait for gossipOK to become true, or until serverContext is canceled.
		if !s.gossip.gossipOK {
			log.Printf("waiting for node to join...")
			s.gossip.gossSignal.Wait() // Wait until gossipOK becomes true

			if s.serverContext.Err() != nil {
				log.Printf("gossip process exiting due to context cancellation")
				s.gossip.gossMu.Unlock()
				return
			}
		}
		s.gossip.gossipOK = s.startGossipProcess()

		s.gossip.gossMu.Unlock()

	}
}

func (s *GBServer) startGossipProcess() bool {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Printf("Gossip process started")
	for {
		select {
		case <-s.serverContext.Done():
			log.Printf("Gossip process stopped due to context cancellation")
			return false
		case <-ticker.C:
			// Perform gossiping tasks here
			log.Printf("Gossip processing at %s", time.Now())
		case gossipState := <-s.gossip.gossipControlChannel:
			if !gossipState {
				// If gossipControlChannel sends 'false', stop the gossiping process
				log.Printf("Gossip control received false, stopping gossip process")
				return false
			}
		}
	}
}

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
	paValue    float64 // Not to be gossiped
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

// -- TODO do we need to think about comparisons here?
// Thread safe
func (s *GBServer) addParticipantFromTmp(name string, tmpP *tmpParticipant) error {

	s.clusterMapLock.Lock()

	// First add to the cluster map
	s.clusterMap.participants[name] = &Participant{
		name:       name,
		keyValues:  tmpP.keyValues,
		valueIndex: tmpP.vi,
	}

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

	cm := s.clusterMap.partIndex
	parts := s.clusterMap.participants

	idx := 0
	for _, v := range cm {
		// Lock the participant to safely read the data
		value := parts[v]

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
