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
	gossWg               sync.WaitGroup
}

func initGossipSettings(gossipInterval time.Duration, nodeSelection uint8) *gossip {

	// TODO Will need to carefully incorporate with config or flags

	goss := &gossip{
		gossInterval:         gossipInterval,
		nodeSelection:        nodeSelection,
		gossipControlChannel: make(chan bool, 1),
		gossipOK:             false,
		gossWg:               sync.WaitGroup{},
	}

	goss.gossSignal = sync.NewCond(&goss.gossMu)

	return goss
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
	index     int
	key       string
	valueType byte // Type could be internal, config, state, client
	version   int64
	value     []byte // Value should go last for easier de-serialisation
	// Could add user defined metadata later on??
}

type deltaQueue struct {
	index   int
	key     string
	version int64
}

type deltaHeap []*deltaQueue

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
	// TODO Move cluster map lock from server to here
}

type participantQueue struct {
	index           int
	name            string
	availableDeltas int
	maxVersion      int64 // Simply reference from deltaQ as maxVersion
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
	ph[i].index, ph[j].index = i, j
}

//goland:noinspection GoMixedReceiverTypes
func (ph *participantHeap) Push(x interface{}) {
	n := len(*ph)
	item := x.(*participantQueue)
	item.index = n
	*ph = append(*ph, item)
}

//goland:noinspection GoMixedReceiverTypes
func (ph *participantHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	x.index = -1
	*ph = old[0 : n-1]
	return x
}

// Lock needs to be held on entry
//
//goland:noinspection GoMixedReceiverTypes
func (ph *participantHeap) update(item *participantQueue, availableDeltas int, maxVersion int64, name ...string) {

	if name != nil && len(name) == 1 {
		item.name = name[0]
	}

	item.availableDeltas = availableDeltas
	item.maxVersion = maxVersion

	heap.Fix(ph, item.index)

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
	dh[i].index, dh[j].index = i, j
}

//goland:noinspection GoMixedReceiverTypes
func (dh *deltaHeap) Push(x interface{}) {
	n := len(*dh)
	item := x.(*deltaQueue)
	item.index = n
	*dh = append(*dh, item)
}

//goland:noinspection GoMixedReceiverTypes
func (dh *deltaHeap) Pop() interface{} {
	old := *dh
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	x.index = -1
	*dh = old[0 : n-1]
	return x
}

//goland:noinspection GoMixedReceiverTypes
func (dh *deltaHeap) update(item *Delta, version int64, key ...string) {

	item.version = version
	if len(key) == 1 {
		item.key = key[0]
	}

	heap.Fix(dh, item.index)

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

//=======================================================
// Participant Handling

// Cluster Lock must be held on entry
func (p *Participant) getMaxVersion() (int64, error) {

	maxVersion := p.deltaQ[0]
	if maxVersion == nil {
		return 0, nil
	}

	return maxVersion.version, nil

}

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
		dq := &deltaQueue{
			key:     key,
			version: delta.version,
		}
		heap.Push(&newParticipant.deltaQ, dq)
	}

	// Add the participant to the map
	s.clusterMap.participants[name] = newParticipant

	mv, err := newParticipant.getMaxVersion()
	if err != nil {
		return err
	}

	// Add to the participant heap
	s.clusterMap.participantQ.Push(&participantQueue{
		name:            name,
		availableDeltas: 0,
		maxVersion:      mv,
	})

	// Clear tmpParticipant references
	tmpP.keyValues = nil
	tmpP.vi = nil

	s.clusterMapLock.Unlock()

	return nil
}

//=======================================================
// Delta Comparisons
//=======================================================

// TODO Consider a digest pool to use to ease pressure on the Garbage Collector
// TODO We could serialise directly from the cluster map and make a byte digest - the receiver will then only have to build a tmpDigest

// Thread safe
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

//=======================================================
// Gossip Syn Prep
//=======================================================

func (s *GBServer) sendDigest(conn *gbClient) (int, error) {

	ctx, cancel := context.WithTimeout(s.serverContext, 2*time.Second)
	defer cancel()

	// Generate the digest - if it's above MTU we either return a subset OR we move to streaming in which we'll need
	// Adjust node header
	digest, err := s.generateDigest()
	if err != nil {
		return 0, fmt.Errorf("sendDigest - generate digest error: %v", err)
	}

	reqID, err := s.acquireReqID()
	if err != nil {
		return 0, fmt.Errorf("sendDigest - acquiring ID error: %v", err)
	}

	header := constructNodeHeader(1, GOSS_SYN, reqID, uint16(len(digest)), NODE_HEADER_SIZE_V1, 0, 0)
	packet := &nodePacket{
		header,
		digest,
	}
	cereal, err := packet.serialize()
	if err != nil {
		return 0, fmt.Errorf("sendDigest - serialize error: %v", err)
	}

	// Now we queue with response
	resp, err := conn.qProtoWithResponse(ctx, cereal, false, true)
	if err != nil {
		return 0, fmt.Errorf("sendDigest - qProtoWithResponse error: %v", err)
	}

	log.Printf("response = %s", resp)

	return 1, nil

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
				log.Printf("%s - gossip process exiting due to context cancellation", s.ServerName)
				s.gossip.gossMu.Unlock()
				return
			}

		}

		if s.serverContext.Err() != nil {
			log.Printf("%s - STOP!!!!", s.ServerName)
			s.gossip.gossMu.Unlock()
			return
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
	atomic.AddInt64(&s.gossip.isGossiping, 1)
}

func (s *GBServer) decrementGossip() {
	newValue := atomic.AddInt64(&s.gossip.isGossiping, -1)
	if newValue == 0 {
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
			log.Printf("%s - Gossip process stopped due to context cancellation - waiting for rounds to finish", s.ServerName)
			s.endGossip()
			s.gossip.gossWg.Wait() // Wait for the rounds to finish
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
				s.gossip.gossWg.Wait() // Wait for the rounds to finish
				return false
			}
		}
	}
}

//--------------------
// Gossip Round
// TODO Need to look at gossip level context + gossip round context

func (s *GBServer) StartGossipRound() error {

	s.serverLock.Lock()
	if s.flags.isSet(SHUTTING_DOWN) {
		s.serverLock.Unlock()
		return fmt.Errorf("shutting down")
	}
	s.serverLock.Unlock()

	//if s.ServerID.uuid == 1 {
	//	err := s.selectNodeAndGossip()
	//	if err != nil {
	//		return err
	//	}
	//}

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
	//s.clusterMapLock.RLock()
	//defer s.clusterMapLock.RUnlock()

	ns := s.gossip.nodeSelection
	pl := len(s.clusterMap.participantQ) - 1

	if int(ns) > pl {
		// Or pl = 1
		return fmt.Errorf("gossip process stopped not enough nodes to select")
	}

	gossipingWith := make([]string, ns)

	// Trying error channel approach here to collect any node specific error during gossip

	for i := 0; i < int(ns); i++ {

		select {
		case <-s.serverContext.Done():
			return nil
		default:
			s.clusterMapLock.RLock()
			num := rand.Intn(pl) + 1
			node := s.clusterMap.participantQ[num]

			// TODO Check exist and through error if not
			conn, exists := s.nodeStore[node.name]
			s.clusterMapLock.RUnlock()

			// Need to check if we have dialed the connection
			if !exists {
				log.Printf("no connection - skipping")
			}

			// Increment before starting a new gossip operation
			s.incrementGossip()
			// Increment the wait group for this gossip operation
			s.gossip.gossWg.Add(1)

			// Random delay to break symmetry
			//delay := time.Duration(rand.Intn(100)) * time.Millisecond
			//time.Sleep(delay)

			gossipingWith[i] = node.name
			log.Printf("gossiping with %v", gossipingWith[i])

			s.startGoRoutine(s.ServerName, "gossip-round", func() {
				defer s.decrementGossip() // Decrement after gossip with the node completes
				defer s.gossip.gossWg.Done()

				// Gossip with the node and collect any errors
				if err := s.gossipWithNode(node.name, conn); err != nil {
				}
			})

		}

	}

	// Collect and aggregate errors

	return nil
}

//TODO When a new node joins - it is given info by the seed - so when choosing to gossip - for some it will need to dial
// the nodes using the addr in the clusterMap

func (s *GBServer) gossipWithNode(node string, conn *gbClient) error {
	log.Printf("%s -- gossiping with %s - addr %s", s.ServerName, node, conn.gbc.RemoteAddr())

	// TODO send test messages over connections

	// Will be using a state machine approach
	// - moving through each stage and checking ctx.Err() && other checks before continuing

	testMsg := []byte("Hello there pretend i am a digest message\r\n")

	id, err := s.acquireReqID()
	if err != nil {
		return err
	}

	header := constructNodeHeader(1, GOSS_SYN, id, uint16(len(testMsg)), NODE_HEADER_SIZE_V1, 0, 0)

	packet := &nodePacket{
		header,
		testMsg,
	}
	pay1, err := packet.serialize()
	if err != nil {
		log.Printf("Error serializing packet: %v", err)
	}

	// TODO So the response is the problem right now
	//
	resp, err := conn.qProtoWithResponse(s.serverContext, pay1, true, true)
	if err != nil {
		log.Printf("Error in gossip round: %v", err)
	}

	//conn.mu.Lock()
	//conn.qProto(pay1, true)
	//conn.mu.Unlock()

	//
	log.Printf("%s resp: %s", s.ServerName, resp)

	return nil

}
