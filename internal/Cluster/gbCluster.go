package Cluster

import (
	"GoferBroke/internal/Errors"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
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

// Will need to make a gossip cleanup which we can defer in the main server go-routine

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

func (s *GBServer) gossipCleanup() {

	close(s.gossip.gossipControlChannel)
	close(s.gossip.gossipSemaphore)
	s.gossip.gossipingWith.Clear()

}

//===================================================================================
// Cluster Map
//===================================================================================

/*
====================== CLUSTER MAP LOCKING STRATEGY ======================

🔹 General Locking Rules:
1️⃣ **Always acquire ClusterMap.mu first** when modifying the ClusterMap.
2️⃣ **Release ClusterMap.mu before locking Participant.pm** to prevent deadlocks.
3️⃣ **Never hold ClusterMap.mu while locking Participant.pm** (avoids circular waits).
4️⃣ **Use RWMutex**:
   - `ClusterMap.RLock()` for read operations (e.g., looking up participants).
   - `ClusterMap.Lock()` for write operations (e.g., adding/removing participants).
   - `Participant.pm.Lock()` only for modifying a specific participant.

🔹 Safe Locking Order:
✅ **Read a participant (safe)**
   1. `ClusterMap.RLock()`
   2. Lookup participant
   3. `ClusterMap.RUnlock()`
   4. `Participant.pm.Lock()`
   5. Modify participant
   6. `Participant.pm.Unlock()`

✅ **Modify a participant (safe)**
   1. `ClusterMap.RLock()`
   2. Lookup participant
   3. `ClusterMap.RUnlock()`
   4. `Participant.pm.Lock()`
   5. Modify participant
   6. `Participant.pm.Unlock()`

✅ **Add a participant (safe)**
   1. `ClusterMap.Lock()`
   2. Add participant to map
   3. `ClusterMap.Unlock()`

✅ **Remove a participant (safe)**
   1. `ClusterMap.Lock()`
   2. Lookup participant
   3. Remove from map
   4. `ClusterMap.Unlock()` ✅ (Release before locking `Participant.pm`)
   5. `Participant.pm.Lock()`
   6. Perform cleanup - if needed
   7. `Participant.pm.Unlock()`

🚨 **Avoid Deadlocks:**
❌ **Never do this:** `ClusterMap.Lock()` → `Participant.pm.Lock()`
   - This can cause a circular wait if another goroutine holds `Participant.pm.Lock()`
     and then tries to acquire `ClusterMap.Lock()`.

=======================================================================
*/

// System gossip types
const (
	HEARTBEAT_V = iota
	ADDR_V
	CPU_USAGE_V
	MEMORY_USAGE_V
	NUM_NODE_CONN_V
	NUM_CLIENT_CONN_V
	STRING_V
)

// Internal Delta Keys [NOT TO BE USED EXTERNALLY]
const (
	_ADDRESS_         = "TCP"
	_CPU_USAGE_       = "CPU_USAGE"
	_MEMORY_USAGE     = "MEMORY_USAGE"
	_NODE_CONNS_      = "NODE_CONNS"
	_CLIENT_CONNS_    = "CLIENT_CONNS"
	_HEARTBEAT_       = "HEARTBEAT"
	_UPDATE_INTERVAL_ = "UPDATE_INTERVAL"
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

// deltaQueue is used during gossip exchanges when a node receives a digest to compare against its cluster map it will
// select deltas from participants that are most outdated. Deltas are then added to a temporary heap in order to sort by max-version
// before serialising
type deltaQueue struct {
	index    int
	overload bool
	key      string
	version  int64
}

type deltaHeap []*deltaQueue

type Participant struct {
	name      string // Possibly can remove
	keyValues map[string]*Delta
	//paDetection *phiAccrual
	maxVersion int64
	pm         sync.RWMutex
}

type ClusterMap struct {
	seedServer   *Seed
	participants map[string]*Participant

	// TODO Need to find a more efficient way of selecting random nodes to gossip with from the map rather than storing an array
	participantArray []string
	// TODO Move cluster map lock from server to here
}

type participantQueue struct {
	index           int
	name            string
	availableDeltas int
	maxVersion      int64
}

type participantHeap []*participantQueue

//-------------------
//Heartbeat Monitoring + Failure Detection

type phiControl struct {
	threshold    int
	windowSize   int
	phiSemaphore chan struct{}
	phiControl   chan bool
	phiOK        bool
	mu           sync.Mutex
}

type phiAccrual struct {
	lastBeat    int64
	currentBeat int64
	window      []int64
	windowIndex int
	pa          sync.Mutex
}

//=======================================================
// Participant Heap
//=======================================================

//goland:noinspection GoMixedReceiverTypes
func (ph participantHeap) Len() int {
	return len(ph)
}

//goland:noinspection GoMixedReceiverTypes
func (ph participantHeap) Less(i, j int) bool {
	return ph[i].maxVersion > ph[j].maxVersion
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
	return dh[i].version < dh[j].version
}

//goland:noinspection GoMixedReceiverTypes
func (dh deltaHeap) Swap(i, j int) {
	dh[i], dh[j] = dh[j], dh[i]
	dh[i].index, dh[j].index = i, j
}

//goland:noinspection GoMixedReceiverTypes
func (dh *deltaHeap) Push(x any) {
	n := len(*dh)
	item := x.(*deltaQueue)
	item.index = n
	*dh = append(*dh, item)
}

//goland:noinspection GoMixedReceiverTypes
func (dh *deltaHeap) Pop() any {
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

	if len(key) == 1 {
		item.key = key[0]
	}

	item.version = version

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
		make([]string, 0),
	}

	// We don't add the phiAccrual here as we don't track our own internal failure detection

	cm.participants[name] = participant
	cm.participantArray = append(cm.participantArray, participant.name)

	return cm

}

//--------
//Update cluster

func (s *GBServer) addGSADeltaToMap(delta *clusterDelta) error {

	// If we receive information about ourselves, we need to ignore as we are the only ones who should update information about ourselves

	s.clusterMapLock.RLock()
	cm := s.clusterMap.participants
	s.clusterMapLock.RUnlock()

	for name, d := range delta.delta {

		if name == s.ServerName {
			continue
		}
		if participant, exists := cm[name]; exists {
			// Here we have a participant with values we need to update for our map
			// Use the locking strategy to lock participant in order to update
			for k, v := range d.keyValues {

				if deltaV, exists := participant.keyValues[k]; exists {

					if v.version > deltaV.version {
						participant.pm.Lock()
						deltaV.version = v.version
						participant.pm.Unlock()
					}

				} else {
					participant.pm.Lock()
					participant.keyValues[k] = v
					participant.pm.Unlock()
				}
			}
		}
		// If here then we have a new participant to add
		err := s.addParticipantFromTmp(name, d)
		if err != nil {
			return err
		}
	}
	return nil
}

//--

//=======================================================
// Participant Handling

//Add/Remove Participant

// -- TODO Look at weak pointers for tmpP in order to release to GC as soon as possible
// -- TODO do we need to think about comparisons here?
// Thread safe
func (s *GBServer) addParticipantFromTmp(name string, tmpP *tmpParticipant) error {

	s.clusterMapLock.Lock()

	// Step 1: Add participant to the participants map
	newParticipant := &Participant{
		name:      name,
		keyValues: make(map[string]*Delta), // Allocate a new map
	}

	var maxV int64

	// Deep copy each key-value pair
	for k, v := range tmpP.keyValues {

		valueByte := make([]byte, len(v.value))
		copy(valueByte, v.value)

		if v.version > maxV {
			maxV = v.version
		}

		newParticipant.keyValues[k] = &Delta{
			index:     v.index,
			key:       v.key,
			valueType: v.valueType,
			version:   v.version,
			value:     valueByte, // Copy value slice
		}
	}

	newParticipant.maxVersion = maxV

	s.clusterMap.participants[name] = newParticipant
	s.clusterMap.participantArray = append(s.clusterMap.participantArray, newParticipant.name)

	// Clear tmpParticipant references
	tmpP.keyValues = nil

	s.clusterMapLock.Unlock()

	return nil
}

func (s *GBServer) addDiscoveryToMap(name string, disc *discoveryValues) error {

	s.clusterMapLock.Lock()
	defer s.clusterMapLock.Unlock()

	if _, exists := s.clusterMap.participants[name]; exists {

		part := s.clusterMap.participants[name]

		for addrKey, addrValue := range disc.addr {
			if _, exist := part.keyValues[addrKey]; exist {

				//Clear reference to discovery for faster GC collection
				disc = nil

				return fmt.Errorf("key %s already exists - %w", addrKey, Errors.AddDiscoveryErr)
			}

			valueByte := make([]byte, len(addrValue))
			copy(valueByte, addrValue)

			// If the address is not present then we add and let the value be updated along with the version during gossip
			part.keyValues[addrKey] = &Delta{
				key:       addrKey,
				valueType: ADDR_V,
				version:   0,
				value:     valueByte,
			}
		}

		//Clear reference to discovery for faster GC collection
		disc = nil

		return nil
	}

	newParticipant := &Participant{
		name:      name,
		keyValues: make(map[string]*Delta),
	}

	for addrKey, addrValue := range disc.addr {

		valueByte := make([]byte, len(addrValue))
		copy(valueByte, addrValue)

		newParticipant.keyValues[addrKey] = &Delta{
			key:       addrKey,
			valueType: ADDR_V,
			version:   0,
			value:     valueByte,
		}

	}

	s.clusterMap.participants[name] = newParticipant
	s.clusterMap.participantArray = append(s.clusterMap.participantArray, newParticipant.name)

	//Clear reference to discovery for faster GC collection
	disc = nil

	return nil

}

//=======================================================
// Discovery Phase
//=======================================================

// Discovery Request for node during discovery phase - will take the gossip rounds context and timeout
// TODO - Should this be in the node file as only nodes will be making requests - responses are then general to the cluster ? OR keep it together?
func (s *GBServer) discoveryRequest(ctx context.Context, conn *gbClient) ([]byte, error) {

	//TODO we are doing _address_ checks in the serialiser but we may want something more robust to check standard tcp address known
	// but also preferred address and address groups from config...?
	knownNodes, err := s.getKnownAddressNodes()
	if err != nil {
		return nil, fmt.Errorf("discoveryRequest - getKnownAddressNodes failed: %s", err)
	}

	dreq, err := s.serialiseKnownAddressNodes(knownNodes)
	if err != nil {
		// TODO Need to error handle serialisers
		return nil, Errors.WrapGBError(Errors.DiscoveryReqErr, err)
	}

	reqId, err := s.acquireReqID()
	if err != nil {
		return nil, Errors.WrapGBError(Errors.DiscoveryReqErr, err)
	}

	pay, err := prepareRequest(dreq, 1, DISCOVERY_REQ, reqId, 0)
	if err != nil {
		return nil, fmt.Errorf("%w, %w", Errors.DiscoveryReqErr, err)
	}

	resp := conn.qProtoWithResponse(reqId, pay, false)

	r, err := conn.waitForResponseAndBlock(ctx, resp)
	if err != nil {
		return nil, Errors.WrapGBError(Errors.DiscoveryReqErr, err)
	}

	return r.msg, nil

}

func (s *GBServer) conductDiscovery(ctx context.Context, conn *gbClient) error {

	resp, err := s.discoveryRequest(ctx, conn)
	if err != nil {
		return err
	}

	// If we are assume we have a response
	addrNodes, err := deserialiseDiscovery(resp)
	if err != nil {
		return err
	}

	// Add to our map
	for name, value := range addrNodes.dv {
		err := s.addDiscoveryToMap(name, value)
		if err != nil {
			return err
		}
	}

	// Do a check for proportion missing
	current := int(s.numNodeConnections) + len(addrNodes.dv) + 1 // Plus ourselves

	perc := percMakeup(current, int(addrNodes.addrCount))

	if s.gbConfig.Cluster.discoveryPercentage != 0 {
		if perc >= int(s.gbConfig.Cluster.discoveryPercentage) {
			s.discoveryPhase = false
		}
	} else if perc >= DEFAULT_DISCOVERY_PERCENTAGE {
		s.discoveryPhase = false
	}

	// Come out of discovery or not

	return nil
}

func (c *gbClient) discoveryResponse(request []string) ([]byte, error) {

	addrMap, err := c.srv.buildAddrGroupMap(request[1:])
	if err != nil {
		return nil, Errors.WrapGBError(Errors.DiscoveryReqErr, err)
	}

	if len(addrMap) == 0 {
		return nil, Errors.EmptyAddrMapErr
	}

	cereal, err := c.srv.serialiseDiscoveryAddrs(addrMap)
	if err != nil {
		return nil, Errors.WrapGBError(Errors.DiscoveryReqErr, err)
	}

	return cereal, nil
}

//=======================================================
// Phi Accrual Failure Detection
//=======================================================

func initPhiControl() *phiControl {

	// Should be taken from config
	return &phiControl{
		10,
		50,
		make(chan struct{}),
		make(chan bool),
		false,
		sync.Mutex{},
	}

}

func (s *GBServer) tryStartPhiProcess() bool {
	if s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {
		log.Printf("%s - Cannot start phi: shutting down or context canceled", s.ServerName)
		return false
	}

	select {
	case s.phi.phiSemaphore <- struct{}{}:
		s.flags.set(PHI_STARTED)
		return true
	default:
		return false
	}
}

func (s *GBServer) endPhiProcess() {
	select {
	case <-s.phi.phiSemaphore:
	default:
	}
	s.flags.clear(PHI_STARTED)
}

func (s *GBServer) phiProcess(ctx context.Context) {
	// Need to wait for the gossip signal to be signalled - here we use the gossipMutex which is the mutex passed to the sync.Cond
	// Also use a defer condition for contextFunc()

	stopCondition := context.AfterFunc(ctx, func() {
		// Notify all waiting goroutines to proceed if needed.
		s.gossip.gossSignal.L.Lock()
		defer s.gossip.gossSignal.L.Unlock()
		s.flags.clear(PHI_STARTED)
		s.flags.set(PHI_EXITED)
	})
	defer stopCondition()

	// Then start ticker process
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {

		s.gossip.gossMu.Lock()

		if s.serverContext.Err() != nil {
			log.Printf("%s - gossip process exiting due to context cancellation", s.ServerName)
			//s.endGossip()
			s.gossip.gossMu.Unlock()
			return
		}

		// Wait for gossipOK to become true, or until serverContext is canceled.
		if !s.phi.phiOK || !s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {

			log.Printf("waiting for gossip signal...")
			s.gossip.gossSignal.Wait() // Wait until gossipOK becomes true

		}

		if s.flags.isSet(SHUTTING_DOWN) || s.serverContext.Err() != nil {
			log.Printf("PHI - SHUTTING DOWN")
			s.gossip.gossMu.Unlock()
			return
		}

		s.gossip.gossMu.Unlock()

		s.phi.phiOK = s.startPhiProcess()
		//log.Printf("running phi check")

	}

}

func (s *GBServer) startPhiProcess() bool {

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.serverContext.Done():
			log.Printf("phi process exiting due to context cancellation")
			return false
		case ctrl := <-s.phi.phiControl:
			if !ctrl {
				log.Printf("phi stopped through control channel ------------")
				return false
			}
		case <-ticker.C:
			log.Printf("running phi check")
		}
	}

}

func (p *phiAccrual) recordHeartbeat() {

}

//=======================================================================
// Preparing Cluster Map for Gossip Exchanges with Depth + Flow-Control
//=======================================================================

//=======================================================
// GOSS_SYN Prep
//=======================================================

//------------------
// Generate Digest

// Thread safe
func (s *GBServer) generateDigest() ([]byte, int, error) {

	// We need to determine if we have too many participants then we need to enter a subset strategy selection
	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	// First we need to make an estimate to see if the current number of participants in our map would exceed an MTU_DIGEST allocation
	// Serialisation header bytes minus 1,max node name len max(23), 8 for every participant
	// If we are over then we must create a subset
	mtuEstimate := CEREAL_DIGEST_HEADER_SIZE + (len(cm.participants) * 32)
	if mtuEstimate > MTU_DIGEST {
		// TODO Move the subset creation within here
		// Create a subset here
		if len(cm.participantArray) > 10 {

			var newPartArray []string

			selectedNodes := make(map[string]struct{}, len(newPartArray))
			subsetSize := 0

			// Weak pointer to newArray? clean up after?

			for i := 0; i < 10; i++ {
				randNum := rand.Intn(len(cm.participantArray))
				node := cm.participantArray[randNum]

				if exists := cm.participants[node]; exists != nil {

					if _, ok := selectedNodes[node]; ok {
						continue
					}

					if subsetSize > MTU_DIGEST {
						break
					}

					newPartArray = append(newPartArray, node)
					selectedNodes[node] = struct{}{}
					subsetSize += 1 + len(node) + 8
				}

			}

			// Pass the newPartArray to the serialiseClusterDigestWithArray
			cereal, err := s.serialiseClusterDigestWithArray(newPartArray, subsetSize)
			if err != nil {
				return nil, 0, err
			}
			return cereal, subsetSize, nil
		}
	}

	if s.debugTrack == 1 {
		b, size, err := s.serialiseClusterDigest()
		if err != nil {
			return nil, 0, err
		}
		return b, size, nil
	}

	b, size, err := s.serialiseClusterDigest()
	if err != nil {
		return nil, 0, err
	}

	// If not, we need to decide if we will stream or do a random subset of participants (increasing propagation time)

	return b, size, nil
}

func (s *GBServer) modifyDigest(digest []byte) ([]byte, int, error) {

	if bytes.HasSuffix(digest, []byte(CLRF)) {
		digest = digest[:len(digest)-2]
		binary.BigEndian.PutUint32(digest[1:5], uint32(len(digest)))
	}

	digestLen := int(binary.BigEndian.Uint32(digest[1:5]))
	if int(digestLen) != len(digest) {
		return nil, 0, fmt.Errorf("modifyDigest - digest length mismatch")
	}

	return digest, digestLen, nil

}

//-------------------------
// Send Digest in GOSS_SYN - Stage 1

func (s *GBServer) sendDigest(ctx context.Context, conn *gbClient) (responsePayload, error) {
	// Generate the digest
	digest, _, err := s.generateDigest()
	if err != nil {
		return responsePayload{}, fmt.Errorf("sendDigest - generate digest error: %w", err)
	}

	// Acquire request ID
	reqID, err := s.acquireReqID()
	if err != nil {
		return responsePayload{}, fmt.Errorf("sendDigest - acquiring ID error: %w", err)
	}

	// Construct the packet
	header := constructNodeHeader(1, GOSS_SYN, reqID, 0, uint16(len(digest)), NODE_HEADER_SIZE_V1)
	packet := &nodePacket{
		header,
		digest,
	}
	cereal, gbErr := packet.serialize()
	if gbErr != nil {
		return responsePayload{}, fmt.Errorf("sendDigest - serialize error: %w", gbErr)
	}

	select {
	case <-ctx.Done():
		return responsePayload{}, fmt.Errorf("sendDigest - context canceled before sending digest: %w", ctx.Err())
	default:

	}

	// Send the digest and wait for a response
	resp := conn.qProtoWithResponse(reqID, cereal, false)

	r, err := conn.waitForResponseAndBlock(ctx, resp)
	if err != nil {
		return responsePayload{}, err
	}

	return r, nil

}

//=======================================================
// GOSS_SYN_ACK Prep
//=======================================================

func (s *GBServer) generateParticipantHeap(sender string, digest *fullDigest) (ph participantHeap, err error) {

	// Here we are not looking at participants we are missing - that will be sent to us when we send our digest
	// We are focusing on what we have that the other node does not based on their digest we are receiving

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	// Initialise an empty participantHeap as a value so as not to overwrite when we gossip with other nodes concurrently

	partQueue := make(participantHeap, 0, len(cm.participants))

	for name, participant := range cm.participants {

		available := 0

		// First check if we have a higher max version than the digest received + if we have a participant they do not
		if peerDigest, exists := (*digest)[name]; exists {

			if peerDigest.nodeName == sender {
				continue
			}

			// If the participant is included in the digest then we to check the versions
			// We must compare int here and not int64 as I found out after hours of headaches
			if int(participant.maxVersion) > int(peerDigest.maxVersion) {
				available += len(participant.keyValues)
			}
		} else if name != sender {
			available += len(participant.keyValues)
		}

		if available > 0 {
			// We add the participant to the participant heap
			heap.Push(&partQueue, &participantQueue{
				name:            name,
				availableDeltas: available,
				maxVersion:      participant.maxVersion,
			})
		}

	}
	// Initialise the heap here will order participants by most outdated and then by available deltas
	heap.Init(&partQueue)

	if len(partQueue) == 0 {
		return nil, Errors.EmptyParticipantHeapErr
	}

	return partQueue, nil

}

func (s *GBServer) buildDelta(ph *participantHeap, remaining int) (finalDelta map[string][]*Delta, size int, err error) {

	// Need to go through each participant in the heap - add each delta to a heap order it - pop each delta
	// and get it's size + node + metadata if within bounds, we can either serialise here OR
	// we can store selected deltas in a map against node keys and return them along with a size ready to be passed
	// to serialiser

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	sizeOfDelta := 0

	// TODO Would prefer to pre-allocate if there is a way -- need to think about this here
	selectedDeltas := make(map[string][]*Delta)

	// TODO This is should have a set ceiling to avoid open ended loops running away from us
	for ph.Len() > 0 && sizeOfDelta < remaining {

		phEntry := heap.Pop(ph).(*participantQueue)
		participant := cm.participants[phEntry.name]

		// Make a delta heap for each participant
		dh := make(deltaHeap, 0, len(participant.keyValues))
		for _, delta := range participant.keyValues {

			// TODO We also want to track and trace overloaded delta to see and monitor

			overloaded := len(delta.value) > DEFAULT_MAX_DELTA_SIZE

			// Overloaded check and trace here (only if they are gossiped?)

			heap.Push(&dh, &deltaQueue{
				key:      delta.key,
				version:  delta.version,
				overload: overloaded,
			})

		}
		heap.Init(&dh) // Initialise the heap, so we can pop most outdated version and process

		// We are to send the highest version deltas, and we fit as many deltas in based on remaining space.

		// First add the participant size to the sizeOfDelta
		sizeOfDelta += 1 + len(participant.name) + 2 // 1 byte for name length + name + size of delta key-values

		deltaList := make([]*Delta, 0)

		// Make selected delta list here and populate

		for dh.Len() > 0 {

			d := heap.Pop(&dh).(*deltaQueue)
			delta := participant.keyValues[d.key]
			// Calc size to do a remaining check before committing to selecting
			size := 14 + len(d.key) + len(delta.value)

			if size+sizeOfDelta > remaining {
				break
			}

			sizeOfDelta += size

			deltaList = append(deltaList, delta)

		}

		if len(deltaList) > 0 {
			selectedDeltas[participant.name] = deltaList
		}

	}

	return selectedDeltas, sizeOfDelta, nil
}

// TODO So during this process I want to log or track the amount of times we go over MTU - so log as warning but also take the count along with the trace of where and when

// TODO Change output to byte as we will be serialising the delta after comparing and populating the queues
func (s *GBServer) prepareGossSynAck(sender string, digest *fullDigest) ([]byte, error) {

	// Prepare our own digest first as we need to know if digest reaches its cap, so we know how much space we have left for the Delta
	d, _, err := s.generateDigest()
	if err != nil {
		return nil, fmt.Errorf("compareAndBuildDelta - generate digest error: %w", err)
	}

	// Compare here - Will need to take a remaining size left over from generating our digest
	partQueue, err := s.generateParticipantHeap(sender, digest)
	if err != nil {

		handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {
			if len(gbError) > 0 {
				return gbError[0]
			} else {
				return err
			}
		})

		if errors.Is(handledErr, Errors.EmptyParticipantHeapErr) {

			return d, nil
		}
		return nil, nil

	}

	//Modify digest if no error
	newD, newSize, err := s.modifyDigest(d)
	if err != nil {
		return nil, err
	}

	remaining := DEFAULT_MAX_GSA - newSize

	// Populate delta queues and build selected deltas
	selectedDeltas, deltaSize, err := s.buildDelta(&partQueue, remaining)
	if err != nil {
		return nil, err
	}

	// Serialise
	cereal, err := s.serialiseGSA(newD, selectedDeltas, deltaSize)

	// Return
	// TODO somewhere after this another 13 10 gets added to the delta buff causing a length mismatch
	return cereal, nil
}

//-------------------------
// Send Digest And Delta in GOSS_SYN_ACK - Stage 2

func (c *gbClient) sendGossSynAck(sender string, digest *fullDigest) error {

	reqID := c.ph.reqID

	// serialise and send?
	gsa, err := c.srv.prepareGossSynAck(sender, digest)
	if err != nil {
		return err
	}

	respID, err := c.srv.acquireReqID()
	if err != nil {
		return err
	}

	pay, err := prepareRequest(gsa, 1, GOSS_SYN_ACK, reqID, respID)
	if err != nil {
		return err
	}

	//log.Printf("%s --> sent GSA - waiting for response async", c.srv.ServerName)

	ctx, cancel := context.WithTimeout(c.srv.serverContext, 5*time.Second)

	resp := c.qProtoWithResponse(respID, pay, false)

	c.waitForResponseAsync(ctx, resp, func(delta responsePayload, err error) {

		defer cancel()
		if err != nil {
			log.Printf("%s - error in sending goss_syn_ack: %v", c.srv.ServerName, err.Error())
			// Need to handle the error but for now just return
			return
		}

		// If we receive a response delta - process and add it to our map
		srv := c.srv

		// TODO When we reach here - at this point we can have a corrupt max-version - it is also only on seed-server for all nodes
		log.Printf("%s - delta in async [][][][][][][][][] = %s", srv.ServerName, delta.msg)

		cd, e := deserialiseDelta(delta.msg)
		if e != nil {
			log.Printf("error in sending goss_syn_ack: %v", e)
			return
		}
		e = srv.addGSADeltaToMap(cd)
		if e != nil {
			log.Printf("error in sending goss_syn_ack: %v", e)
			return
		}

		return

	})

	return nil
}

//=======================================================
// GOSS_ACK Prep
//=======================================================

func (s *GBServer) prepareACK(sender string, fd *fullDigest) ([]byte, error) {

	// Compare here - Will need to take a remaining size left over from generating our digest
	partQueue, err := s.generateParticipantHeap(sender, fd)
	if err != nil {

		handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {
			if len(gbError) > 0 {
				return gbError[0]
			} else {
				return err
			}
		})
		if errors.Is(handledErr, Errors.EmptyParticipantHeapErr) {
			return nil, Errors.WrapGBError(Errors.GossAckErr, Errors.EmptyParticipantHeapErr)
		}
		return nil, nil

	}

	remaining := DEFAULT_MAX_GSA

	// Populate delta queues and build selected deltas
	selectedDeltas, deltaSize, err := s.buildDelta(&partQueue, remaining)
	if err != nil {
		return nil, err
	}

	delta, err := s.serialiseACKDelta(selectedDeltas, deltaSize)
	if err != nil {
		return nil, Errors.WrapGBError(Errors.GossAckErr, err)
	}

	return delta, nil

}

//========================================================================================
// GOSSIP
//========================================================================================

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
		s.phi.phiControl <- false
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

		s.gossip.gossMu.Unlock()

		// now safe to call
		ok := s.startGossipProcess()

		// optionally re-acquire if needed
		s.gossip.gossMu.Lock()
		s.gossip.gossipOK = ok
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

				continue
			}

			// Create a context for the gossip round
			ctx, cancel := context.WithTimeout(s.serverContext, 4*time.Second)

			// go s.runPhiCheck()

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
				// Pause the phi process also
				s.phi.phiControl <- false
				s.gossip.gossWg.Wait() // Wait for the rounds to finish
				return false
			}
		}
	}
}

//=======================================================
// Gossip Round
//=======================================================

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
	pl := len(s.clusterMap.participantArray) - 1

	if int(ns) > pl {
		log.Printf("OH NOOOOOOOO")
		return
	}

	//for discoveryPhase we want to be quick here and not hold up the gossip round - so we conduct discovery and exit
	//if s.discoveryPhase {
	//	//	// TODO Need a better load seed mechanism
	//	conn, ok := s.nodeConnStore.Load(s.clusterMap.participantArray[1])
	//	if !ok {
	//		log.Printf("No connection to discover addresses in the map")
	//	}
	//
	//	seed, ok := conn.(*gbClient)
	//	if ok {
	//		err := s.conductDiscovery(s.serverContext, seed)
	//		if err != nil {
	//			handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {
	//
	//				if len(gbError) > 1 {
	//					log.Printf("gb error = %v", gbError[2])
	//					return gbError[2]
	//				}
	//				return err
	//			})
	//
	//			if errors.Is(handledErr, Errors.EmptyAddrMapNetworkErr) {
	//				log.Printf("%s - exiting discovery phase", s.ServerName)
	//				s.discoveryPhase = false
	//			} else {
	//				log.Printf("Discovery failed: %v", err)
	//			}
	//
	//		}
	//		return
	//	}
	//	return
	//}

	// Channel to signal when individual gossip tasks complete
	done := make(chan struct{}, pl)
	// Channels aren't like files; you don't usually need to close them.
	// Closing is only necessary when the receiver must be told there are no more values coming, such as to terminate a range loop.
	// https://go.dev/tour/concurrency/4#:~:text=Note%3A%20Only%20the%20sender%20should,to%20terminate%20a%20range%20loop.

	//defer close(done) // Either defer close here and have: v, ok := <-done check before signalling or don't close

	for i := 0; i < int(ns); i++ {

		s.clusterMapLock.RLock()

		num := rand.Intn(pl) + 1
		selectedNode := s.clusterMap.participantArray[num]

		// TODO Check exist and dial if not --> then throw error if neither work
		s.clusterMapLock.RUnlock()

		// TODO We need to collect the errors and make a decision on what to do with them based on the error itself
		s.startGoRoutine(s.ServerName, fmt.Sprintf("gossip-round-%v", i), func() {
			defer func() {
				done <- struct{}{}

				s.updateSelfInfo(time.Now().Unix(), func(participant *Participant, timeOfUpdate int64) error {
					err := updateHeartBeat(participant, timeOfUpdate)
					if err != nil {
						log.Printf("Error updating heartbeat: %v", err)
					}
					return nil
				})

			}()

			s.gossipWithNode(ctx, selectedNode)
		})

	}

	// Wait for all tasks or context cancellation
	for i := 0; i < int(ns); i++ {
		select {
		case <-ctx.Done():
			log.Printf("%s - Gossip round canceled: %v", s.ServerName, ctx.Err())
			return
		case <-done:
			//log.Printf("%s - Node gossip task completed", s.ServerName)
			return
		}
	}

	return

}

//TODO Need to decide how to propagate errors

func (s *GBServer) gossipWithNode(ctx context.Context, node string) {

	defer func() {
		s.endGossip()
		s.clearGossipingWithMap()
	}()

	if s.flags.isSet(SHUTTING_DOWN) {
		log.Printf("pulled from gossip with node")
		return
	}

	//------------- Dial Check -------------//

	//s.serverLock.Lock()
	_, exists, err := s.getNodeConnFromStore(node)
	// We unlock here and let the dial methods re-acquire the lock if needed - we don't assume we will need it
	//s.serverLock.Unlock()
	// TODO Fix nil pointer deference here
	if err == nil && !exists {
		log.Printf("DIALLING")
		err := s.connectToNodeInMap(ctx, node)
		if err != nil {
			log.Printf("error in gossip with node %s ----> %v", node, err)
			return
		}
		return
	} else if err != nil {
		log.Printf("error in gossip with node %s ----> %v", node, err)
		return
	}

	err = s.storeGossipingWith(node)
	if err != nil {
		return
	}

	var stage int

	// So what we can do is set up a progress and error collection

	//------------- GOSS_SYN Stage 1 -------------//

	// TODO The max version seems to have caused a problem + discovery is also a problem + we dialled for some reason
	// TODO Need to clean up and make more clear the goss stages and req-resp cycle

	stage = 1
	log.Printf("%s - Gossiping with node %s (stage %d)", s.ServerName, node, stage)

	//// Stage 1: Send Digest
	//resp, err := s.sendDigest(ctx, conn)
	//if err != nil {
	//	if errors.Is(err, context.Canceled) {
	//		log.Printf("%s - Gossip round canceled at stage %d: %v", s.ServerName, stage, err)
	//	} else {
	//		log.Printf("Error in gossip round (stage %d): %v", stage, err)
	//		return
	//	}
	//	return
	//}
	//
	////------------- GOSS_SYN_ACK Stage 2 -------------//
	//stage = 2
	//
	//sender, fdValue, cdValue, err := deserialiseGSA(resp.msg)
	//if err != nil {
	//	log.Printf("deserialise GSA failed: %v", err)
	//}
	//
	//// Use CD Value to add to process and add to out map
	//if cdValue != nil {
	//	err = s.addGSADeltaToMap(cdValue)
	//	if err != nil {
	//		log.Printf("addGSADeltaToMap failed: %v", err)
	//	}
	//}
	//
	////------------- GOSS_ACK Stage 3 -------------//
	//stage = 3
	//
	//ack, err := s.prepareACK(sender, fdValue)
	//if err != nil || ack == nil {
	//	conn.sendErrResp(uint16(0), resp.respID, Errors.NoDigestErr.Error())
	//	return
	//}
	//
	//pay, err := prepareRequest(ack, 1, GOSS_ACK, uint16(0), resp.respID)
	//if err != nil {
	//	return
	//}
	//
	//conn.mu.Lock()
	//conn.enqueueProto(pay)
	//conn.mu.Unlock()

	//------------- Handle Completion -------------

	if err != nil {
		log.Printf("Error in gossip round (stage %d): %v", stage, err)
	}

	// Update Phi Accrual for Failure Detection

	select {
	case <-ctx.Done():
		//log.Printf("%s - Gossip round canceled at stage %d: %v", s.ServerName, stage, ctx.Err())
		return
	default:
		// Signal that this gossip task has completed
		//log.Printf("%s - Gossip with node %s completed successfully", s.ServerName, node)
	}
}

// ======================
