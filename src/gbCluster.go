package src

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
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
	gossipSemaphore      chan struct{}
	gossipingWith        sync.Map
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
		gossipSemaphore:      make(chan struct{}, 1),
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

// TODO We need to return error for this and handle them accordingly
func initClusterMap(name string, seed *net.TCPAddr, participant *Participant) *ClusterMap {

	cm := &ClusterMap{
		&Seed{seedAddr: seed},
		make(map[string]*Participant),
		make(participantHeap, 0),
		make(map[string]*phiAccrual),
	}

	// We don't add the phiAccrual here as we don't track our own internal failure detection

	cm.participants[name] = participant
	mv, err := participant.getMaxVersion()
	if err != nil {
		log.Fatal(err)
	}

	// Create a participantQueue entry and push it to the heap
	pq := &participantQueue{
		name:            name,
		availableDeltas: 0,  // Initialize to 0
		maxVersion:      mv, // Initialize to 0
	}
	heap.Push(&cm.participantQ, pq)

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

	//for k, v := range tmpP.keyValues {
	//	log.Printf("-------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> %s-%+s", k, v.value)
	//}

	// Check ADDR specifically
	//if addr, exists := tmpP.keyValues[_ADDRESS_]; exists {
	//	log.Printf("ADDR in tmpParticipant: Name=%s, ADDR=%s", name, string(addr.value))
	//} else {
	//	log.Printf("ERROR: ADDR missing in tmpParticipant for %s!", name)
	//}

	// Step 1: Add participant to the participants map
	newParticipant := &Participant{
		name:      name,
		keyValues: make(map[string]*Delta), // Allocate a new map
		deltaQ:    make(deltaHeap, 0),
	}

	// Deep copy each key-value pair
	for k, v := range tmpP.keyValues {

		valueByte := make([]byte, len(v.value))
		copy(valueByte, v.value)

		newParticipant.keyValues[k] = &Delta{
			index:     v.index,
			key:       v.key,
			valueType: v.valueType,
			version:   v.version,
			value:     valueByte, // Copy value slice
		}
	}

	// Populate the delta heap
	for key, delta := range tmpP.keyValues {
		dq := &deltaQueue{
			key:     key,
			version: delta.version,
		}
		heap.Push(&newParticipant.deltaQ, dq)
	}

	log.Printf("--------------------------------------------------Before adding to map: Name=%s, Addr=%s", name, tmpP.keyValues["ADDR"].value)
	s.clusterMap.participants[name] = newParticipant
	//log.Printf("---------------------------------------------------------------------------------After adding to map: Name=%s, Addr=%s", name, s.clusterMap.participants[name].keyValues["ADDR"].value)

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

//=======================================================
// GOSS_SYN Prep
//=======================================================

//------------------
// Generate Digest

// TODO Consider a digest pool to use to ease pressure on the Garbage Collector
// TODO We could serialise directly from the cluster map and make a byte digest - the receiver will then only have to build a tmpDigest

// Thread safe
func (s *GBServer) generateDigest() ([]byte, error) {

	s.clusterMapLock.Lock()
	// First we need to determine if the Digest fits within MTU

	// Check how many participants first - anymore than 40 will definitely exceed MTU
	if len(s.clusterMap.participantQ) > 40 {
		return nil, fmt.Errorf("too many participants")
		// TODO Implement overfill strategy for digest
	}

	// TODO We need to run max version lazy updates on participants we have selected to be in the digest

	b, err := s.serialiseClusterDigest()
	if err != nil {
		return nil, err
	}

	// Check len against MTU
	if len(b) > MTU {
		return nil, fmt.Errorf("MTU exceeded")
		// TODO Implement overfill strategy for digest
	}

	s.clusterMapLock.Unlock()

	// If not, we need to decide if we will stream or do a random subset of participants (increasing propagation time)

	return b, nil
}

//-------------------------
// Send Digest in GOSS_SYN - Stage 1

func (s *GBServer) sendDigest(ctx context.Context, conn *gbClient) ([]byte, error) {
	// Generate the digest
	digest, err := s.generateDigest()
	if err != nil {
		return nil, fmt.Errorf("sendDigest - generate digest error: %w", err)
	}

	// Acquire request ID
	reqID, err := s.acquireReqID()
	if err != nil {
		return nil, fmt.Errorf("sendDigest - acquiring ID error: %w", err)
	}

	// Construct the packet
	header := constructNodeHeader(1, GOSS_SYN, reqID, uint16(len(digest)), NODE_HEADER_SIZE_V1, 0, 0)
	packet := &nodePacket{
		header,
		digest,
	}
	cereal, err := packet.serialize()
	if err != nil {
		return nil, fmt.Errorf("sendDigest - serialize error: %w", err)
	}

	// Introduce an artificial delay before sending the request
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("sendDigest - context canceled before sending digest: %w", ctx.Err())
	default:

	}

	// Send the digest and wait for a response
	resp, err := conn.qProtoWithResponse(ctx, cereal, true, true)
	if err != nil {
		// Explicitly handle context cancellations
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			log.Printf("sendDigest - context canceled for node: %v", err)
			return nil, err
		}

		return nil, fmt.Errorf("sendDigest - qProtoWithResponse error: %w", err)
	}

	return resp, nil
}

//=======================================================
// Gossip Signalling + Process
//=======================================================

//TODO to avoid bouncing gossip between two nodes - server should flag what node it is gossiping with
// If it selects a node to gossip with and it is the one it is currently gossiping with then it should pick another or wait

//----------------
//Gossip Signalling

// TODO when we start the node again the flags need to be reset so gossip_signalled should be clear here

// Thread safe
func (s *GBServer) checkGossipCondition() {
	nodes := atomic.LoadInt64(&s.numNodeConnections)

	if nodes >= 1 && !s.flags.isSet(GOSSIP_SIGNALLED) {
		s.serverLock.Lock()
		s.flags.set(GOSSIP_SIGNALLED)
		s.serverLock.Unlock()
		s.gossip.gossipControlChannel <- true
		log.Println("signalling gossip")
		s.gossip.gossSignal.Broadcast()

	} else if nodes < 1 && s.flags.isSet(GOSSIP_SIGNALLED) {
		s.gossip.gossipControlChannel <- false
		s.serverLock.Lock()
		s.flags.clear(GOSSIP_SIGNALLED)
		s.serverLock.Unlock()
	}

}

//----------------
//Gossip sync.Map

func (s *GBServer) storeGossipingWith(node string) error {

	timestampStr := node[len(node)-10:]

	// Convert the extracted string to an integer
	nodeTimestamp, err := strconv.Atoi(timestampStr)
	if err != nil {
		return fmt.Errorf("failed to convert timestamp '%s' to int: %v", timestampStr, err)
	}

	s.gossip.gossipingWith.Store(node, nodeTimestamp)
	//log.Println("storing --> ", node)

	return nil
}

func (s *GBServer) getGossipingWith(node string) (int, error) {
	nodeSeniority, exists := s.gossip.gossipingWith.Load(node)
	if exists {
		// Use type assertion to convert the value to an int
		if seniority, ok := nodeSeniority.(int); ok {
			return seniority, nil
		}
		return 0, fmt.Errorf("type assertion failed for node: %s", node)
	}
	return 0, fmt.Errorf("node %s not found", node)
}

func (s *GBServer) clearGossipingWithMap() {
	//log.Printf("clearing map")
	s.gossip.gossipingWith.Clear()
}

func (s *GBServer) deferGossipRound(node string) (bool, error) {

	// Compare both nodes ID and timestamp data to determine what node has seniority in the gossip exchange and what
	// node needs to defer

	nodeTime, exists := s.gossip.gossipingWith.Load(node)
	if !exists {
		return false, nil // Don't need the error to be returned here as we will be continuing with gossip
	}

	nt, ok := nodeTime.(int)
	if !ok {
		return false, fmt.Errorf("node value type error - expected int, got %T", nodeTime)
	}

	s.serverLock.RLock()

	serverTime := int(s.timeUnix)

	s.serverLock.RUnlock()

	if serverTime == nt {
		log.Printf("timestamps are the same - skipping round")
		return true, nil
	}

	if serverTime > nt {
		//log.Printf("%s time tester = %v-%v", s.ServerName, serverTime, nt)
		return true, nil
	} else {
		return false, nil
	}

}

//----------------
//Gossip Control

func (s *GBServer) gossipProcess(ctx context.Context) {
	stopCondition := context.AfterFunc(ctx, func() {
		// Notify all waiting goroutines to proceed if needed.
		s.gossip.gossSignal.L.Lock()
		defer s.gossip.gossSignal.L.Unlock()
		s.flags.clear(GOSSIP_SIGNALLED)
		s.flags.set(GOSSIP_EXITED)
		//close(s.gossip.gossipControlChannel)
		//close(s.gossip.gossipSemaphore)
		s.gossip.gossSignal.Broadcast()
	})
	defer stopCondition()

	for {
		s.gossip.gossMu.Lock()

		if s.serverContext.Err() != nil {
			log.Printf("%s - gossip process exiting due to context cancellation", s.ServerName)
			//s.endGossip()
			s.gossip.gossMu.Unlock()
			return
		}

		// Wait for gossipOK to become true, or until serverContext is canceled.
		if !s.gossip.gossipOK || !s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {

			log.Printf("waiting for node to join...")
			s.gossip.gossSignal.Wait() // Wait until gossipOK becomes true

		}

		if s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {
			log.Printf("WE ARE SHUTTING DOWN")
			s.gossip.gossMu.Unlock()
			return
		}

		s.gossip.gossipOK = s.startGossipProcess()
		//s.gossip.gossipOK = true // For removing the gossip process to test other parts of the system
		s.gossip.gossMu.Unlock()

	}
}

//----------------
//Gossip Check

func (s *GBServer) tryStartGossip() bool {
	// Check if shutting down or context is canceled before attempting gossip
	if s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {
		log.Printf("%s - Cannot start gossip: shutting down or context canceled", s.ServerName)
		return false
	}

	// Attempt to acquire the semaphore
	select {
	case s.gossip.gossipSemaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *GBServer) endGossip() {
	select {
	case <-s.gossip.gossipSemaphore:
	default:
	}
}

//----------------
//Gossip Process

// TODO We need start gossip process to return false in order for us to kick back up to main gossip process and exit or wait

func (s *GBServer) startGossipProcess() bool {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Exit immediately if the server context is already canceled
	if s.serverContext.Err() != nil {
		log.Printf("%s - Server context already canceled, exiting gossip process", s.ServerName)
		return false
	}

	log.Printf("Gossip process started")
	for {
		select {
		case <-s.serverContext.Done():
			log.Printf("%s - Gossip process stopped due to context cancellation - waiting for rounds to finish", s.ServerName)
			s.gossip.gossWg.Wait() // Wait for the rounds to finish
			s.endGossip()          // Ensure state is reset
			return false
		case <-ticker.C:

			// Check if context cancellation occurred
			if s.serverContext.Err() != nil || s.flags.isSet(SHUTTING_DOWN) {
				log.Printf("%s - Server shutting down during ticker event", s.ServerName)
				s.gossip.gossWg.Wait()
				s.endGossip()
				return false
			}

			// Attempt to start a new gossip round
			if !s.tryStartGossip() {

				if s.flags.isSet(SHUTTING_DOWN) {
					log.Printf("%s - Gossip process is shutting down, exiting", s.ServerName)
					return false
				}

				// TODO Currently the context.Err() is nil when we get stuck in the skip loop

				log.Printf("%s - Skipping gossip round because a round is already active", s.ServerName)
				log.Printf("logging the context and shutting down signal here because im confused %v %v", s.serverContext.Err(), s.flags.isSet(SHUTTING_DOWN))

				continue
			}

			// Create a context for the gossip round
			ctx, cancel := context.WithTimeout(s.serverContext, 4*time.Second)

			// Start a new gossip round
			s.gossip.gossWg.Add(1)
			s.startGoRoutine(s.ServerName, "main-gossip-round", func() {
				defer s.gossip.gossWg.Done()
				defer cancel()

				s.startGossipRound(ctx)

			})

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

//TODO ISSUE --> Sometimes gossip round gets caught in a skipping round loop which has something
// to do with the tryGossip checks on isGossiping as well as the concurrency of context cancellation between nodes
// e.g - when running TestGossipSignal test - shutting down node 2 will sometimes leave node 2 in a skipping round loop
// Narrowed down to the problem being that we need to kick back up to the main gossip process by return false on startGossipProcess()
// Doing that is the difficult bit because sometimes we don't detect the ctx signal or the tryGossip check loops
// See Logs:

//TODO ISSUE UPDATE --> Turns out that sometimes shutdown isn't being registered as called under certain conditions meaning the
// server is unaware of a shutdown happening - shutdown could be signalled again..?

// 2025/01/25 10:29:39 test-server-2@1737800976 - Gossip process stopped due to context cancellation - waiting for rounds to finish
//2025/01/25 10:29:39 test-server-2@1737800976 - Gossip already inactive
//2025/01/25 10:29:39 test-server-2@1737800976 - gossip process exiting due to context cancellation
//2025/01/25 10:29:39 test-server-1@1737800975 - Gossip started // TODO <-- test-server-1 misses it's ctx signal
//2025/01/25 10:29:39 test-server-1@1737800975 - Gossiping with node test-server-2@1737800976 (stage 1)
//2025/01/25 10:29:40 test-server-1@1737800975 - Gossip already active
//2025/01/25 10:29:40 test-server-1@1737800975 - Skipping gossip round because a round is already active
//2025/01/25 10:29:41 test-server-1@1737800975 - Gossip already active
//2025/01/25 10:29:41 test-server-1@1737800975 - Skipping gossip round because a round is already active
//2025/01/25 10:29:43 waitForResponse - context canceled for response ID 1
//2025/01/25 10:29:43 responseCleanup - cleaned up response ID 1
//2025/01/25 10:29:43 test-server-1@1737800975 - Gossip already active
//2025/01/25 10:29:43 sendDigest - context canceled for node: context deadline exceeded
//2025/01/25 10:29:43 test-server-1@1737800975 - Skipping gossip round because a round is already active
//2025/01/25 10:29:43 Error in gossip round (stage 1): context deadline exceeded
//2025/01/25 10:29:43 test-server-1@1737800975 - Gossip round canceled: context deadline exceeded
//2025/01/25 10:29:43 test-server-1@1737800975 - Gossip ended
//2025/01/25 10:29:43 test-server-1@1737800975 - Gossip already inactive
//2025/01/25 10:29:43 test-server-1@1737800975 - Gossip already inactive
//2025/01/25 10:29:44 test-server-1@1737800975 - Gossip started // TODO <-- We shouldn't be starting again here

// TODO Think about introducing a gossip result struct for a channel to feed back errors
// type GossipResult struct {
//    Node string
//    Err  error
//}

func (s *GBServer) startGossipRound(ctx context.Context) {

	if s.flags.isSet(SHUTTING_DOWN) {
		log.Printf("server shutting down - exiting gossip round")
		return
	}

	defer s.endGossip() // Ensure gossip state is reset when the process ends
	defer s.clearGossipingWithMap()

	ns := s.gossip.nodeSelection
	pl := len(s.clusterMap.participantQ) - 1

	if int(ns) > pl {
		// Or pl = 1
		return
	}

	// Channel to signal when individual gossip tasks complete
	done := make(chan struct{}, pl)
	// Channels aren't like files; you don't usually need to close them.
	// Closing is only necessary when the receiver must be told there are no more values coming, such as to terminate a range loop.
	// https://go.dev/tour/concurrency/4#:~:text=Note%3A%20Only%20the%20sender%20should,to%20terminate%20a%20range%20loop.

	//defer close(done) // Either defer close here and have: v, ok := <-done check before signalling or don't close

	for i := 0; i < int(ns); i++ {

		s.clusterMapLock.Lock()
		num := rand.Intn(pl) + 1
		node := s.clusterMap.participantQ[num]

		// TODO Check exist and dial if not --> then throw error if neither work
		s.clusterMapLock.Unlock()

		s.startGoRoutine(s.ServerName, fmt.Sprintf("gossip-round-%v", i), func() {
			defer func() { done <- struct{}{} }()

			s.gossipWithNode(ctx, node.name)
		})

	}

	// Wait for all tasks or context cancellation
	for i := 0; i < int(ns); i++ {
		select {
		case <-ctx.Done():
			log.Printf("%s - Gossip round canceled: %v", s.ServerName, ctx.Err())
			return
		case <-done:
			log.Printf("%s - Node gossip task completed", s.ServerName)
		}
	}

	log.Printf("%s gossip round complete", s.ServerName)

	return

}

//TODO When a new node joins - it is given info by the seed - so when choosing to gossip - for some it will need to dial
// the nodes using the addr in the clusterMap

func (s *GBServer) gossipWithNode(ctx context.Context, node string) {

	if s.flags.isSet(SHUTTING_DOWN) {
		log.Printf("pulled from gossip with node")
		return
	}

	//------------- Dial Check -------------//

	//TODO Look at respIDs as a req->resp cycle can only be 1-1. After a completed cycle a new resp ID must be acquired
	// This means that a node responding to a request cannot generate a new ID to put in the header as it must echo the one received first

	//s.serverLock.Lock()
	conn, exists := s.nodeStore[node]
	// We unlock here and let the dial methods re-acquire the lock if needed - we don't assume we will need it
	//s.serverLock.Unlock()
	if !exists {
		log.Printf("%s - Node %s not found in gossip store =================================", s.ServerName, node)

		err := s.connectToNodeInMap(ctx, node)
		if err != nil {
			log.Printf("error in gossip with node %s ----> %v", conn.gbc.RemoteAddr(), err)
			return
		}
		return
	}

	err := s.storeGossipingWith(node)
	if err != nil {
		return
	}

	var stage int

	//------------- GOSS_SYN Stage 1 -------------//

	stage = 1
	log.Printf("%s - Gossiping with node %s (stage %d)", s.ServerName, node, stage)

	//// Stage 1: Send Digest
	//s.clusterMapLock.Lock()
	resp, err := s.sendDigest(ctx, conn)
	//s.clusterMapLock.Unlock()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Printf("%s - Gossip round canceled at stage %d: %v", s.ServerName, stage, err)
		} else {
			log.Printf("Error in gossip round (stage %d): %v", stage, err)
			s.endGossip()
			s.clearGossipingWithMap()
			return
		}
		return
	}

	log.Printf("%s - Response received from node %s: %s", s.ServerName, node, resp)

	//------------- Handle Completion -------------

	select {
	case <-ctx.Done():
		log.Printf("%s - Gossip round canceled at stage %d: %v", s.ServerName, stage, ctx.Err())
		return
	default:
		// Signal that this gossip task has completed
		log.Printf("%s - Gossip with node %s completed successfully", s.ServerName, node)
	}
}

// ======================
