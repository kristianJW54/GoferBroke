package cluster

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//===================================================================================
// Gossip
//===================================================================================

// TODO Draft struct for gossip metrics which will drive events for warning etc and maintain gossip cluster health
type gossipMetrics struct {
	bytesPerRound   int
	roundSkips      int
	inFlightErrors  int
	mtuReached      int
	avgExchangeTime int
}

type gossip struct {
	gossInterval         time.Duration
	nodeSelection        uint8 // Remove and use node config instead OR use config to populate this
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
	D_STRING_TYPE = iota
	D_BYTE_TYPE
	D_INT_TYPE
	D_INT8_TYPE
	D_INT64_TYPE
	D_INT16_TYPE
	D_INT32_TYPE
	D_UINT8_TYPE
	D_UINT16_TYPE
	D_UINT32_TYPE
	D_UINT64_TYPE
	D_FLOAT64_TYPE
	D_BOOL_TYPE
)

// Internal Delta Keys [NOT TO BE USED EXTERNALLY]
const (
	_ADDRESS_         = "tcp"
	_REACHABLE_       = "reachability"
	_CPU_USAGE_       = "cpu_usage"
	_MEMORY_USAGE     = "memory_usage"
	_NODE_CONNS_      = "node_conns"
	_CLIENT_CONNS_    = "client_conns"
	_HEARTBEAT_       = "heartbeat"
	_UPDATE_INTERVAL_ = "update_interval"
	_NETWORK_LOAD_    = "network_load"
	_NODE_NAME_       = "node_name"
)

// Standard Delta Key-Groups
const (
	ADDR_DKG      = "address"
	SYSTEM_DKG    = "system"
	NETWORK_DKG   = "network"
	LOCAL_LOG_DKG = "local_log"
	CONFIG_DKG    = "config"
	TEST_DKG      = "test"
)

const (
	DELTA_META_SIZE = 15
)

type Seed struct {
	seedAddr *net.TCPAddr
}

//-------------------
// Main cluster map for gossiping

//TODO For flow-control we will need to add the rates to the header of a digest exchange
// therefore we need to think where we track and store the rates - in a struct, embedded or calculated and included in the serialised digest on the fly

type Delta struct {
	index     int
	KeyGroup  string
	Key       string
	Version   int64
	ValueType byte   // This should be string, int, int64, float64 etc
	Value     []byte // Value should go last for easier de-serialisation
	// Could add user defined metadata later on??
}

// deltaQueue is used during gossip exchanges when a node receives a digest to compare against its cluster map it will
// select deltas from participants that are most outdated. Deltas are then added to a temporary heap in order to sort by max-version
// before serialising
type deltaQueue struct {
	index    int
	overload bool
	keyGroup string
	key      string
	version  int64
}

type connectionMetaData struct {
	inboundSuccess    bool
	advertisedAddr    *net.TCPAddr
	reachableClaim    int
	reachableObserved int
}

type deltaHeap []*deltaQueue

type Participant struct {
	name string
	// Will be changing to delta store interface
	keyValues        map[string]*Delta // composite key - flattened -> group:key
	connection       *connectionMetaData
	paDetection      *phiAccrual
	maxVersion       int64
	configMaxVersion int64
	pm               sync.RWMutex
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
	peerMaxVersion  int64
}

type participantHeap []*participantQueue

//=======================================================
// Participant Heap
//=======================================================

//goland:noinspection GoMixedReceiverTypes
func (ph participantHeap) Len() int {
	return len(ph)
}

//goland:noinspection GoMixedReceiverTypes
func (ph participantHeap) Less(i, j int) bool {
	if ph[i].availableDeltas == ph[j].availableDeltas {
		return ph[i].maxVersion > ph[j].maxVersion
	}
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
		item.Key = key[0]
	}

	item.Version = version

	heap.Fix(dh, item.index)

}

//=======================================================
// Delta Handling - Internal Delta API
//=======================================================

func MakeDeltaKey(group, key string) string {
	return fmt.Sprintf("%s:%s", group, key)
}

func ParseDeltaKey(key string) (string, string) {
	parts := strings.Split(key, ":")
	return parts[0], parts[1]
}

// Store takes a delta value and stores in the participants own cluster map
// Thread safe
func (p *Participant) Store(d *Delta) error {

	// We assume the delta is well-formed and correct

	compKey := MakeDeltaKey(d.KeyGroup, d.Key)

	if delta, exists := p.keyValues[compKey]; exists {

		// If delta already exists we try to update
		err := p.Update(delta.KeyGroup, delta.Key, d, func(toBeUpdated, by *Delta) {
			*toBeUpdated = *by
		})
		if err != nil {
			return err
		}

		return nil

	} else {
		p.pm.Lock()
		p.keyValues[compKey] = d
		p.pm.Unlock()
		return nil
	}
}

func (p *Participant) Update(group, key string, d *Delta, update func(toBeUpdated, by *Delta)) error {

	if delta, exists := p.keyValues[MakeDeltaKey(group, key)]; exists {

		// If either delta key or group is different, we need to dis-regard the update with an error
		if d.KeyGroup != delta.KeyGroup || d.Key != delta.Key {
			return Errors.DeltaUpdateKeyErr
		}
		p.pm.Lock()
		update(delta, d)
		p.pm.Unlock()

		if d.Version > p.maxVersion {
			p.maxVersion = d.Version
		}

		return nil

	} else {
		return Errors.DeltaUpdateNoDeltaErr
	}
}

func (p *Participant) GetAll() map[string]*Delta {
	return p.keyValues
}

func (p *Participant) Get(deltaKey string) (*Delta, error) {

	return nil, nil
}

//=======================================================
// Cluster Map Handling
//=======================================================

//---------------------

// TODO We need to return error for this and handle them accordingly
func initClusterMap(serverName string, seed *net.TCPAddr, participant *Participant) *ClusterMap {

	log.Printf("called")

	cm := &ClusterMap{
		&Seed{seedAddr: seed},
		make(map[string]*Participant),
		make([]string, 0, 4),
	}

	// We don't add the phiAccrual here as we don't track our own internal failure detection

	log.Printf("adding %s to cluster map ", serverName)

	cm.participants[serverName] = participant
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

	for uuid, d := range delta.delta {

		if uuid == s.getID() {
			continue
		}

		if participant, exists := cm[uuid]; exists {

			// Here we have a participant with values we need to update for our map
			// Use the locking strategy to lock participant in order to update
			for k, v := range d.keyValues {

				de := &DeltaUpdateEvent{
					DeltaKey: k,
				}

				// We get our own participants view of the participant in our map and update it
				err := participant.Update(v.KeyGroup, v.Key, v, func(toBeUpdated, by *Delta) {
					if by.Version > toBeUpdated.Version {

						de.PreviousVersion = toBeUpdated.Version
						de.PreviousValue = bytes.Clone(toBeUpdated.Value)

						*toBeUpdated = *by // Shallow copy - Ok as by won't be used elsewhere - it is what was received on the wire

						de.CurrentVersion = toBeUpdated.Version
						de.CurrentValue = bytes.Clone(toBeUpdated.Value)

					}
				})

				// We need to handle the error from the update delta method
				// If we receive no delta found GBError then we know to add as new delta
				// Any other error we know to return
				if err != nil {
					handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {

						if gbError[0] != nil {
							return gbError[0]
						} else {
							return err
						}

					})

					// If the error is no delta found then it is a new delta we must add
					if errors.Is(handledErr, Errors.DeltaUpdateNoDeltaErr) {
						participant.pm.Lock()
						log.Printf("error = %v - storing new delta", handledErr)
						participant.keyValues[k] = v
						participant.pm.Unlock()

						// Event call for new delta added
						// TODO We could use this to build an update rate and inform gossip interval times?
						s.DispatchEvent(Event{
							EventType: NewDeltaAdded,
							Time:      time.Now().Unix(),
							Payload: &DeltaAddedEvent{
								DeltaKey:   k,
								DeltaValue: bytes.Clone(v.Value),
							},
							Message: "New delta added",
						})

					} else {
						return Errors.WrapGBError(Errors.AddGSAErr, err)
					}

				}

				// Event call for delta updated
				s.DispatchEvent(Event{
					DeltaUpdated,
					time.Now().Unix(),
					de,
					"Delta updated",
				})

			}
		} else {

			// If here then we have a new participant to add
			err := s.addParticipantFromTmp(uuid, d)
			if err != nil {
				return err
			}

			// Event call for new participant added

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
		name:        name,
		keyValues:   make(map[string]*Delta), // Allocate a new map
		paDetection: s.initPhiAccrual(),
	}

	var maxV int64

	// Deep copy each key-value pair
	for k, v := range tmpP.keyValues {

		valueByte := make([]byte, len(v.Value))
		copy(valueByte, v.Value)

		if v.Version > maxV {
			maxV = v.Version
		}

		newParticipant.keyValues[k] = &Delta{
			index:     v.index,
			KeyGroup:  v.KeyGroup,
			Key:       v.Key,
			ValueType: v.ValueType,
			Version:   v.Version,
			Value:     valueByte, // Copy value slice
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

			key, group := ParseDeltaKey(addrKey)

			// If the address is not present then we add and let the value be updated along with the version during gossip
			part.keyValues[addrKey] = &Delta{
				Key:       key,
				KeyGroup:  group,
				ValueType: D_STRING_TYPE,
				Version:   0,
				Value:     valueByte,
			}
		}

		//Clear reference to discovery for faster GC collection
		disc = nil

		return nil
	}

	newParticipant := &Participant{
		name:        name,
		keyValues:   make(map[string]*Delta),
		paDetection: s.initPhiAccrual(),
	}

	for addrKey, addrValue := range disc.addr {

		valueByte := make([]byte, len(addrValue))
		copy(valueByte, addrValue)

		newParticipant.keyValues[addrKey] = &Delta{
			Key:       addrKey,
			ValueType: D_STRING_TYPE,
			Version:   0,
			Value:     valueByte,
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

func (s *GBServer) runDiscovery(ctx context.Context) error {

	seed, err := s.retrieveASeedConn(false) // We only retrieve a random seed if there is a lot of cluster load
	if err != nil {
		return err
	}

	err = s.conductDiscovery(ctx, seed)
	if err != nil {
		handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {
			for _, ge := range gbError {
				log.Printf("gb error = %v", ge)
			}
			return gbError[len(gbError)-1]
		})

		if errors.Is(handledErr, Errors.EmptyAddrMapNetworkErr) {
			log.Printf("%s - exiting discovery phase", s.PrettyName())
			s.discoveryPhase = false
		} else {
			log.Printf("discovery phase failed")
		}
	}

	log.Printf("discovery phase active - aborting gossip round")
	return nil

}

// Discovery Request for node during discovery phase - will take the gossip rounds context and timeout
// TODO - Should this be in the node file as only nodes will be making requests - responses are then general to the cluster ? OR keep it together?
func (s *GBServer) discoveryRequest(ctx context.Context, conn *gbClient) ([]byte, error) {

	//TODO we are doing _address_ checks in the serialiser but we may want something more robust to check standard tcp address known
	// but also preferred address and address groups from config...?
	knownNodes, err := s.getKnownAddressNodes()
	if err != nil {
		return nil, fmt.Errorf("discoveryRequest - getKnownAddressNodes failed: %s", err)
	}

	log.Printf("known node = %+s", knownNodes)

	dreq, err := s.serialiseKnownAddressNodes(knownNodes)
	if err != nil {
		// TODO Need to error handle serialisers
		return nil, Errors.WrapGBError(Errors.DiscoveryReqErr, err)
	}

	reqId, err := s.acquireReqID()
	if err != nil {
		return nil, Errors.ChainGBErrorf(
			Errors.DiscoveryReqErr,
			err,
			"failed to acquire request ID",
		)
	}

	pay, err := prepareRequest(dreq, 1, DISCOVERY_REQ, reqId, 0)
	if err != nil {
		return nil, Errors.ChainGBErrorf(
			Errors.DiscoveryReqErr,
			err,
			"failed to prepare discovery request payload with ID %d", reqId,
		)
	}

	// TODO Fix discovery request - payload seems to be ok so check Process discovery request and then see what we get also check our response (doesn't seem to be response right now)

	log.Printf("pay = %v", pay)

	resp := conn.qProtoWithResponse(ctx, reqId, pay, false)

	r, err := conn.waitForResponseAndBlock(resp)
	if err != nil {
		log.Printf("GOT A DISCOVERY ERROR = %s", err.Error())
		return nil, Errors.WrapGBError(Errors.DiscoveryReqErr, err)
	}

	log.Printf("response =============== %v", r)

	return r.msg, nil

}

func (s *GBServer) conductDiscovery(ctx context.Context, conn *gbClient) error {

	log.Printf("%s conducting discovery", s.name)

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

	if s.gbClusterConfig.Cluster.DiscoveryPercentage != 0 {
		if perc >= s.gbClusterConfig.Cluster.DiscoveryPercentage {
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
	resp := conn.qProtoWithResponse(ctx, reqID, cereal, false)

	r, err := conn.waitForResponseAndBlock(resp)
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
		var peerMaxVersion int64 = 0 // default if not found

		// Skip self (sender)
		if name == sender {
			continue
		}

		if peerDigest, exists := (*digest)[name]; exists {
			peerMaxVersion = peerDigest.maxVersion
		}

		// Count only outdated deltas
		for _, delta := range participant.keyValues {
			if delta.Version > peerMaxVersion {
				available++
			}
		}

		//log.Printf("available deltas for %s = %v (peerMaxVersion: %d)", name, available, peerMaxVersion)

		if available > 0 {
			heap.Push(&partQueue, &participantQueue{
				name:            name,
				availableDeltas: available,
				maxVersion:      participant.maxVersion,
				peerMaxVersion:  peerMaxVersion,
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

func (s *GBServer) buildDelta(ph *participantHeap, remaining int) (finalDelta map[string][]Delta, size int, err error) {

	// Need to go through each participant in the heap - add each delta to a heap order it - pop each delta
	// and get it's size + node + metadata if within bounds, we can either serialise here OR
	// we can store selected deltas in a map against node keys and return them along with a size ready to be passed
	// to serialiser

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	sizeOfDelta := 0

	// TODO Would prefer to pre-allocate if there is a way -- need to think about this here
	selectedDeltas := make(map[string][]Delta)

	// TODO This is should have a set ceiling to avoid open ended loops running away from us
	for ph.Len() > 0 && sizeOfDelta < remaining {

		phEntry := heap.Pop(ph).(*participantQueue)
		participant := cm.participants[phEntry.name]

		// Make a delta heap for each participant
		// Are we being inefficient here by pushing all deltas onto the heap everytime?
		dh := make(deltaHeap, 0, len(participant.keyValues))
		for _, delta := range participant.keyValues {

			if delta.Version > phEntry.peerMaxVersion {
				heap.Push(&dh, &deltaQueue{
					keyGroup: delta.KeyGroup,
					key:      delta.Key,
					version:  delta.Version,
					overload: len(delta.Value) > int(s.gbClusterConfig.Cluster.MaxDeltaSize),
				})
			}
		}
		heap.Init(&dh)

		// We are to send the highest version deltas, and we fit as many deltas in based on remaining space.

		// First add the participant size to the sizeOfDelta
		sizeOfDelta += 1 + len(participant.name) + 2 // 1 byte for name length + name + size of delta key-values

		deltaList := make([]Delta, 0, 10)

		// Make selected delta list here and populate

		for dh.Len() > 0 {

			d := heap.Pop(&dh).(*deltaQueue)

			delta := participant.keyValues[MakeDeltaKey(d.keyGroup, d.key)]
			// Calc size to do a remaining check before committing to selecting
			size := DELTA_META_SIZE + len(delta.KeyGroup) + len(delta.Key) + len(delta.Value)

			if size+sizeOfDelta > remaining {
				log.Printf("BROKE SIZE -- %d", size+sizeOfDelta)
				break
			}

			sizeOfDelta += size

			deltaList = append(deltaList, *delta)

		}

		if len(deltaList) > 0 {
			selectedDeltas[participant.name] = deltaList
		}

	}

	return selectedDeltas, sizeOfDelta, nil
}

// TODO So during this process I want to log or track the amount of times we go over MTU - so log as warning but also take the count along with the trace of where and when

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

	remaining := int(DEFAULT_MAX_GSA) - newSize

	// Populate delta queues and build selected deltas
	selectedDeltas, deltaSize, err := s.buildDelta(&partQueue, remaining)
	if err != nil {
		return nil, err
	}

	// Serialise
	cereal, err := s.serialiseGSA(newD, selectedDeltas, deltaSize)

	// Return
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

	ctx, cancel := context.WithTimeout(c.srv.ServerContext, 5*time.Second)

	resp := c.qProtoWithResponse(ctx, respID, pay, false)

	c.waitForResponseAsync(resp, func(delta responsePayload, err error) {

		defer cancel()
		if err != nil {
			log.Printf("%s - error in sending goss_syn_ack: %v", c.srv.PrettyName(), err.Error())
			// Need to handle the error but for now just return
			return
		}

		// If we receive a response delta - process and add it to our map
		srv := c.srv

		log.Printf("%s - delta in async [][][][][][][][][] = %s", srv.PrettyName(), delta.msg)

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
	if fd == nil {
		return nil, Errors.WrapGBError(Errors.GossAckErr, Errors.NoDigestErr)
	}

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

	remaining := int(DEFAULT_MAX_GSA)

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

		if s.ServerContext.Err() != nil {
			log.Printf("%s - gossip process exiting due to context cancellation", s.PrettyName())
			//s.endGossip()
			s.gossip.gossMu.Unlock()
			return
		}

		// Wait for gossipOK to become true, or until serverContext is canceled.
		if !s.gossip.gossipOK || !s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {

			log.Printf("waiting for node to join...")
			s.gossip.gossSignal.Wait() // Wait until gossipOK becomes true

		}

		if s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {
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
	if s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {
		log.Printf("%s - Cannot start gossip: shutting down or context canceled", s.PrettyName())
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
	if s.ServerContext.Err() != nil {
		log.Printf("%s - Server context already canceled, exiting gossip process", s.PrettyName())
		return false
	}

	log.Printf("Gossip process started")
	for {
		select {
		case <-s.ServerContext.Done():
			log.Printf("%s - Gossip process stopped due to context cancellation - waiting for rounds to finish", s.PrettyName())
			s.gossip.gossWg.Wait() // Wait for the rounds to finish
			s.endGossip()          // Ensure state is reset
			return false
		case <-ticker.C:

			// Check if context cancellation occurred
			if s.ServerContext.Err() != nil || s.flags.isSet(SHUTTING_DOWN) {
				log.Printf("%s - Server shutting down during ticker event", s.PrettyName())
				s.gossip.gossWg.Wait()
				s.endGossip()
				return false
			}

			// Attempt to start a new gossip round
			if !s.tryStartGossip() {

				if s.flags.isSet(SHUTTING_DOWN) {
					log.Printf("%s - Gossip process is shutting down, exiting", s.PrettyName())
					return false
				}

				log.Printf("%s - Skipping gossip round because a round is already active", s.PrettyName())

				continue
			}

			ctx, cancel := context.WithTimeout(s.ServerContext, 4*time.Second)
			s.gossip.gossWg.Add(1)
			s.startGossipRound(ctx)
			cancel()

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

func (s *GBServer) startGossipRound(ctx context.Context) {

	start := time.Now()
	defer func() {
		log.Printf("Gossip round took: %.3fms", float64(time.Since(start))/1e6)
		s.endGossip()
		s.clearGossipingWithMap()
		s.gossip.gossWg.Done()
	}()

	//for discoveryPhase we want to be quick here and not hold up the gossip round - so we conduct discovery and exit
	//log.Printf("are we in a discovery phase ====== %v", s.discoveryPhase)
	if s.discoveryPhase {
		go func() {
			err := s.runDiscovery(ctx)
			if err != nil {
				log.Printf("%s - Gossip discovery failed: %v", s.PrettyName(), err)
			}
		}()
	}

	ns := s.gbClusterConfig.getNodeSelection()
	s.clusterMapLock.RLock()
	pl := len(s.clusterMap.participantArray)
	s.clusterMapLock.RUnlock()
	if int(ns) > pl-1 {
		ns = 1
	}

	indexes, err := generateRandomParticipantIndexesForGossip(s.clusterMap.participantArray, int(ns), s.String())
	if err != nil {
		log.Printf("Error generating random participant indexes: %v", err)
		// TODO Need to add error event as its internal system error
		return
	}

	var wg sync.WaitGroup
	// We could add a semaphore channel for maximum amount of nodes allowed to be selected limiting the potential for lots of node gossip round spawns

	for _, idx := range indexes {
		s.clusterMapLock.RLock()
		nodeID := s.clusterMap.participantArray[idx]
		participant := s.clusterMap.participants[nodeID]
		s.clusterMapLock.RUnlock()

		if participant.paDetection.dead {
			// Add event here and also handle with background task
			continue
		}

		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			nodeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			s.updateSelfInfo(time.Now().Unix(), func(p *Participant, t int64) error {
				return updateHeartBeat(p, t)
			})

			s.gossipWithNode(nodeCtx, node)
		}(nodeID)
	}

	// Wait for completion or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		log.Printf("%s - Gossip round canceled: %v", s.PrettyName(), ctx.Err())
	case <-done:
		// Completed successfully
	}

	return

}

//TODO Need to decide how to propagate errors

func (s *GBServer) gossipWithNode(ctx context.Context, node string) {

	if s.flags.isSet(SHUTTING_DOWN) {
		return
	}

	//------------- Dial Check -------------//

	conn, exists, err := s.getNodeConnFromStore(node)
	if err == nil && !exists {
		go func() {
			dialCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := s.connectToNodeInMap(dialCtx, node)
			if err != nil {
				// TODO Need to make GBError and add to an error event
				log.Printf("%s - dial node in map failed: %v", s.PrettyName(), err)
			}
		}()
		return
	}
	if err != nil {
		return
	}

	_ = s.storeGossipingWith(node)

	var stage int

	// So what we can do is set up a progress and error collection

	//------------- GOSS_SYN Stage 1 -------------//

	stage = 1
	log.Printf("%s - Gossiping with node %s (stage %d)", s.PrettyName(), node, stage)

	// Stage 1: Send Digest
	resp, err := s.sendDigest(ctx, conn)
	if err != nil {
		return
	}

	//------------- GOSS_SYN_ACK Stage 2 -------------//
	stage = 2

	sender, fdValue, cdValue, err := deserialiseGSA(resp.msg)
	if err == nil && cdValue != nil {
		_ = s.addGSADeltaToMap(cdValue)
	}

	//------------- GOSS_ACK Stage 3 -------------//
	stage = 3

	ack, err := s.prepareACK(sender, fdValue)
	if err != nil || ack == nil {
		conn.sendErrResp(uint16(0), resp.respID, Errors.NoDigestErr.Net())
		return
	}

	pay, err := prepareRequest(ack, 1, GOSS_ACK, uint16(0), resp.respID)
	if err == nil {
		conn.mu.Lock()
		conn.enqueueProto(pay)
		conn.mu.Unlock()
	}

	//------------- Handle Completion -------------//

	_ = s.recordPhi(node)

}

// ======================
