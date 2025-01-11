package src

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"math/rand"
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
	gossipTimeout        time.Duration
	gossipOK             bool
	isGossiping          int64
	gossSignal           *sync.Cond
	gossMu               sync.RWMutex
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

// Lock is held on entry
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

func (s *GBServer) tryStartGossip() bool {

	if atomic.LoadInt64(&s.gossip.isGossiping) == 1 {
		return false
	} else {
		return true
	}
}

func (s *GBServer) incrementGossip() {
	// Increment the counter when a new gossip operation starts
	atomic.AddInt64(&s.gossip.isGossiping, 1)
}

func (s *GBServer) decrementGossip() {
	// Decrement the counter when a gossip operation ends
	if atomic.AddInt64(&s.gossip.isGossiping, -1) == 0 {
		log.Printf("Gossiping completed: All rounds finished")
	}
}

func (s *GBServer) endGossip() {
	// Reset gossipActive to 0
	atomic.StoreInt64(&s.gossip.isGossiping, 0)
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
			if !s.tryStartGossip() {
				log.Printf("Skipping gossip round because a round is already active")
				continue
			}

			err := s.StartGossipRound()
			if err != nil {
				log.Printf("Error in gossip round: %v", err)
			}

		case gossipState := <-s.gossip.gossipControlChannel:
			if !gossipState {
				// If gossipControlChannel sends 'false', stop the gossiping process
				log.Printf("Gossip control received false, stopping gossip process")
				return false
			}
		}
	}
}

//--------------------
// Gossip Round

func (s *GBServer) StartGossipRound() error {

	err := s.selectNodeAndGossip()
	if err != nil {
		return err
	}

	return nil

}

//--------------------
// Gossip Selection

func (s *GBServer) selectNodeAndGossip() error {

	// TODO Need a fail safe check so that we don't gossip with ourselves - check name against our own?

	// We need to select a node and check we are not already gossiping with them.
	s.clusterMapLock.RLock()
	defer s.clusterMapLock.RUnlock()

	ns := s.gossip.nodeSelection
	pl := len(s.clusterMap.participantQ) - 1

	if int(ns) > pl {
		// Or pl = 1
		return fmt.Errorf("gossip process stopped not enough nodes to select")
	}

	for i := 0; i < int(ns); i++ {

		num := rand.Intn(pl) + 1
		node := s.clusterMap.participantQ[num]

		// Increment before starting a new gossip operation
		s.incrementGossip()

		//go s.gossipWithNode(ctx, node)
		s.startGoRoutine(s.ServerName, "gossip-round", func() {
			defer s.decrementGossip() // Decrement after gossip with the node completes
			s.gossipWithNode(node.name)
		})
	}

	// Then we need to access their connection
	return nil
}

func (s *GBServer) gossipWithNode(node string) {
	log.Printf("%s -- gossiping with %s", s.ServerName, s.clusterMap.participants[node].name)

	select {
	case <-s.serverContext.Done():
		log.Printf("Gossip with %s timed out", node)
		return
	case <-time.After(2 * time.Second): // Simulate gossip work
		log.Printf("Completed gossip with %s", node)
		return
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

// TODO Need to majorly optimise cluster map for reduced memory - fast lookup and sorted storing

type Delta struct {
	key       string
	valueType byte // Type could be internal, config, state, client
	version   int64
	value     []byte // Value should go last for easier de-serialisation
	// Could add user defined metadata later on??
}

type deltaHeap []*Delta

type Participant struct {
	name      string // Possibly can remove
	keyValues map[string]*Delta
	deltaQ    deltaHeap // This should be kept sorted to the highest version
	paValue   float64   // Not to be gossiped
	pm        sync.RWMutex
}

type ClusterMap struct {
	seedServer   *Seed
	participants map[string]*Participant
	participantQ participantHeap
	phiAccMap    map[string]*phiAccrual
}

type participantQueue struct {
	name            string
	availableDeltas int
	maxVersion      int64 // Simply pop from deltaQ as maxVersion
}

type participantHeap []*participantQueue

//-------------------
//Heartbeat Monitoring + Failure Detection

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
// Participant Heap
//=======================================================

//goland:noinspection GoMixedReceiverTypes
func (ph participantHeap) Len() int {
	return len(ph)
}

//goland:noinspection GoMixedReceiverTypes
func (ph participantHeap) Less(i, j int) bool {
	return ph[i].availableDeltas > ph[j].availableDeltas
}

//goland:noinspection GoMixedReceiverTypes
func (ph participantHeap) Swap(i, j int) {
	ph[i], ph[j] = ph[j], ph[i]
}

//goland:noinspection GoMixedReceiverTypes
func (ph *participantHeap) Push(x interface{}) {
	*ph = append(*ph, x.(*participantQueue))
}

//goland:noinspection GoMixedReceiverTypes
func (ph *participantHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	*ph = old[0 : n-1]
	return x
}

//=======================================================
// Delta Heap
//=======================================================

//goland:noinspection GoMixedReceiverTypes
func (dh deltaHeap) Len() int {
	return len(dh)
}

//goland:noinspection GoMixedReceiverTypes
func (dh deltaHeap) Less(i, j int) bool {
	return dh[i].version > dh[j].version
}

//goland:noinspection GoMixedReceiverTypes
func (dh deltaHeap) Swap(i, j int) {
	dh[i], dh[j] = dh[j], dh[i]
}

//goland:noinspection GoMixedReceiverTypes
func (dh *deltaHeap) Push(x interface{}) {
	*dh = append(*dh, x.(*Delta))
}

//goland:noinspection GoMixedReceiverTypes
func (dh *deltaHeap) Pop() interface{} {
	old := *dh
	n := len(old)
	x := old[n-1]
	*dh = old[0 : n-1]
	return x
}

//=======================================================
// Delta Handling
//=======================================================

// Lock must be held on entry
func (s *GBServer) getMaxVersion(p *Participant) (int64, error) {

	maxVersion := p.deltaQ[0]
	if maxVersion == nil {
		return 0, nil
	}

	return maxVersion.version, nil

}

//=======================================================
// Cluster Map Handling
//=======================================================

func initClusterMap(name string, seed *net.TCPAddr, participant *Participant) *ClusterMap {

	cm := &ClusterMap{
		&Seed{seedAddr: seed},
		make(map[string]*Participant),
		make(participantHeap, 0),
		make(map[string]*phiAccrual),
	}

	// We don't add the phiAccrual here as we don't track our own internal failure detection

	// Create a participantQueue entry and push it to the heap
	pq := &participantQueue{
		name:            name,
		availableDeltas: 0, // Initialize to 0
		maxVersion:      0, // Initialize to 0
	}
	heap.Push(&cm.participantQ, pq)

	cm.participants[name] = participant

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

	// Step 1: Add participant to the participants map
	newParticipant := &Participant{
		name:      name,
		keyValues: tmpP.keyValues,
		deltaQ:    make(deltaHeap, 0), // Initialize delta heap
	}

	// Populate the delta heap
	for key, delta := range tmpP.keyValues {
		delta.key = key // Ensure the delta knows its key
		heap.Push(&newParticipant.deltaQ, delta)
	}

	// Add the participant to the map
	s.clusterMap.participants[name] = newParticipant

	mv, err := s.getMaxVersion(newParticipant)
	if err != nil {
		return err
	}

	// Add to the participant heap
	s.clusterMap.participantQ.Push(&participantQueue{
		name:            name,
		availableDeltas: 0,
		maxVersion:      mv,
	})

	//s.clusterMap.partIndex = append(s.clusterMap.partIndex, name)

	s.clusterMapLock.Unlock()

	return nil
}

//=======================================================
// Delta Comparisons
//=======================================================

// TODO Consider a digest pool to use to ease pressure on the Garbage Collector
// TODO We could serialise directly from the cluster map and make a byte digest - the receiver will then only have to build a tmpDigest

func (s *GBServer) generateDigest() ([]byte, error) {

	s.clusterMapLock.RLock()
	defer s.clusterMapLock.RUnlock()

	// First we need to determine if the Digest fits within MTU

	// Check how many participants first - anymore than 40 will definitely exceed MTU
	if len(s.clusterMap.participantQ) > 40 {
		return nil, fmt.Errorf("too many participants")
		// TODO Implement overfill strategy for digest
	}

	b, err := s.serialiseClusterDigest()
	if err != nil {
		return nil, err
	}

	// Check len against MTU
	if len(b) > MTU {
		return nil, fmt.Errorf("MTU exceeded")
		// TODO Implement overfill strategy for digest
	}

	// If not, we need to decide if we will stream or do a random subset of participants (increasing propagation time)

	return b, nil
}

//--------
//Compare Digest

//=======================================================
// Delta Parsing, Handling and Changes
//=======================================================
