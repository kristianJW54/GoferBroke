package cluster

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log/slog"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//===================================================================================
// Gossip
//===================================================================================

// gossip holds the logic to run the gossip engine.
type gossip struct {
	gossInterval         time.Duration
	nodeSelection        uint8
	gossipControlChannel chan bool
	gossipTimeout        time.Duration
	gossipOK             bool
	gossipPaused         bool
	gossipSemaphore      chan struct{}
	gossipingWith        sync.Map
	gossSignal           *sync.Cond
	gossMu               sync.RWMutex
	gossWg               sync.WaitGroup
}

func initGossipSettings(gossipInterval time.Duration, nodeSelection uint8) *gossip {

	goss := &gossip{
		gossInterval:         gossipInterval,
		nodeSelection:        nodeSelection,
		gossipControlChannel: make(chan bool, 1),
		gossipOK:             false,
		gossipPaused:         false,
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
1️. **Always acquire ClusterMap.mu first** when modifying the ClusterMap.
2️. **Release ClusterMap.mu before locking Participant.pm** to prevent deadlocks.
3️. **Never hold ClusterMap.mu while locking Participant.pm** (avoids circular waits).
4️. **Use RWMutex**:
   - `ClusterMap.RLock()` for read operations (e.g., looking up participants).
   - `ClusterMap.Lock()` for write operations (e.g., adding/removing participants).
   - `Participant.pm.Lock()` only for modifying a specific participant.

🔹 Safe Locking Order:
  **Read a participant (safe)**
   1. `ClusterMap.RLock()`
   2. Lookup participant
   3. `ClusterMap.RUnlock()`
   4. `Participant.pm.Lock()`
   5. Modify participant
   6. `Participant.pm.Unlock()`

  **Modify a participant (safe)**
   1. `ClusterMap.RLock()`
   2. Lookup participant
   3. `ClusterMap.RUnlock()`
   4. `Participant.pm.Lock()`
   5. Modify participant
   6. `Participant.pm.Unlock()`

  **Add a participant (safe)**
   1. `ClusterMap.Lock()`
   2. Add participant to map
   3. `ClusterMap.Unlock()`

  **Remove a participant (safe)**
   1. `ClusterMap.Lock()`
   2. Lookup participant
   3. Remove from map
   4. `ClusterMap.Unlock()`  (Release before locking `Participant.pm`)
   5. `Participant.pm.Lock()`
   6. Perform cleanup - if needed
   7. `Participant.pm.Unlock()`

  **Avoid Deadlocks:**
  **Never do this:** `ClusterMap.Lock()` → `Participant.pm.Lock()`
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

var deltaTypeToReflect = map[uint8]reflect.Type{
	D_STRING_TYPE:  reflect.TypeOf(""),
	D_BYTE_TYPE:    reflect.TypeOf([]byte{}),
	D_INT_TYPE:     reflect.TypeOf(int(0)),
	D_INT8_TYPE:    reflect.TypeOf(int8(0)),
	D_INT16_TYPE:   reflect.TypeOf(int16(0)),
	D_INT32_TYPE:   reflect.TypeOf(int32(0)),
	D_INT64_TYPE:   reflect.TypeOf(int64(0)),
	D_UINT8_TYPE:   reflect.TypeOf(uint8(0)),
	D_UINT16_TYPE:  reflect.TypeOf(uint16(0)),
	D_UINT32_TYPE:  reflect.TypeOf(uint32(0)),
	D_UINT64_TYPE:  reflect.TypeOf(uint64(0)),
	D_FLOAT64_TYPE: reflect.TypeOf(float64(0)),
	D_BOOL_TYPE:    reflect.TypeOf(true),
}

// Internal Delta Keys [NOT TO BE USED EXTERNALLY]
const (
	_ADDRESS_         = "tcp"
	_REACHABLE_       = "reachability"
	_NODE_CONNS_      = "node_conns"
	_CLIENT_CONNS_    = "client_conns"
	_HEARTBEAT_       = "heartbeat"
	_NODE_NAME_       = "node_name"
	_TOTAL_MEMORY_    = "total_memory"
	_USED_MEMORY_     = "used_memory"
	_FREE_MEMORY_     = "free_memory"
	_MEM_PERC_        = "memory_percent_used"
	_HOST_            = "host"
	_HOST_ID_         = "host_id"
	_CPU_MODE_NAME_   = "cpu_model"
	_CPU_CORES_       = "cpu_cores"
	_PLATFORM_        = "platform"
	_PLATFORM_FAMILY_ = "platform_family"
)

// Standard Delta Key-Groups
const (
	ADDR_DKG      = "address"
	SYSTEM_DKG    = "system"
	NETWORK_DKG   = "network"
	LOCAL_LOG_DKG = "local_log"
	CONFIG_DKG    = "config"
	TEST_DKG      = "test"
	FAILURE_DKG   = "failure"
)

// DELTA_META_SIZE is made up of DELTA_META_SIZE + len(delta.KeyGroup) + len(delta.Key) + len(delta.Value)
// META_SIZE --> |keyGroup Len (u8) |key Len (u8) |Version (u64) |ValueType (u8) |Value Len (u32) |
const (
	DELTA_META_SIZE = 15
)

type Seed struct {
	seedAddr *net.TCPAddr
}

//-------------------------------------------------------------------
// Delta

// Delta holds the single delta as a value inside the KeyValues of a ClusterMap
type Delta struct {
	index     int
	KeyGroup  string
	Key       string
	Version   int64
	ValueType uint8  // This should be string, int, int64, float64 etc
	Value     []byte // Value should go last for easier de-serialisation
	// Could add user defined metadata later on??
}

// deltaQueue is used during gossip exchanges when a node receives a digest to compare against its cluster map it will
// select deltas from participants that are most outdated. Deltas are then added to a temporary heap in order to sort by max-version
// before serialising
type deltaQueue struct {
	delta   *Delta
	version int64
	size    int
	index   int
}

// connectionMetaData is for NAT Traversal
// TODO Implement in future release
type connectionMetaData struct {
	inboundSuccess    bool
	advertisedAddr    *net.TCPAddr
	reachableClaim    int
	reachableObserved int
}

// deltaHeap declares a heap for available deltas which are dynamically added during each round of gossip
type deltaHeap []*deltaQueue

//-------------------------------------------------------------------
// Participant

// Participant is the servers view of a node in the cluster and holds information on its view of that node including
// failure, delta map and max version for efficient diff comparison
type Participant struct {
	name string
	// Will be changing to delta store interface
	keyValues        map[string]*Delta // composite key - flattened -> group:key
	connection       *connectionMetaData
	f                *failure
	maxVersion       int64
	configMaxVersion int64
	pm               sync.RWMutex
}

//-------------------------------------------------------------------
// ClusterMap

// ClusterMap is the main structure and entry point into the Servers ClusterMap. This holds the Servers view of the
// Cluster
type ClusterMap struct {
	seedServer       *Seed
	participants     map[string]*Participant
	participantArray []string
}

type participantQueue struct {
	index           int
	name            string
	availableDeltas int
	maxVersion      int64
	peerMaxVersion  int64
}

// participantHeap holds a list of participants which are out of date and to which we have newer version deltas for.
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

func (dh deltaHeap) Len() int { return len(dh) }

func (dh deltaHeap) Less(i, j int) bool {
	return dh[i].version < dh[j].version // Min-heap by version (Scuttlebutt constraint)
}

func (dh deltaHeap) Swap(i, j int) {
	dh[i], dh[j] = dh[j], dh[i]
	dh[i].index = i
	dh[j].index = j
}

func (dh *deltaHeap) Push(x any) {
	n := len(*dh)
	item := x.(*deltaQueue)
	item.index = n
	*dh = append(*dh, item)
}

func (dh *deltaHeap) Pop() any {
	old := *dh
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	x.index = -1   // for safety
	*dh = old[0 : n-1]
	return x
}

// Optional: If you still need an update method (likely not with direct delta ref)
func (dh *deltaHeap) update(item *deltaQueue, version int64) {
	item.version = version
	heap.Fix(dh, item.index)
}

//=======================================================
// Delta Handling - Internal Delta API
//=======================================================

// MakeDeltaKey is a small helper function which combine a KeyGroup with a Key into KeyGroup:Key to conform to the
// ClusterMap key composition in the keyValues
func MakeDeltaKey(group, key string) string {
	return fmt.Sprintf("%s:%s", group, key)
}

// ParseDeltaKey is a helper function to parse the composite key of a clusterMap 'KeyGroup:Key' key
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
			return Errors.ChainGBErrorf(Errors.StoreDeltaErr, err, "")
		}

		return nil

	} else {
		p.pm.Lock()
		p.keyValues[compKey] = d
		p.pm.Unlock()
		return nil
	}
}

// Update is a callback method where a delta is updated in place by a new delta using a callback specified by the caller
// if the update is successful - Update also tries to update the max version for that Participant
func (p *Participant) Update(group, key string, d *Delta, apply func(toBeUpdated, by *Delta)) error {
	k := MakeDeltaKey(group, key)

	p.pm.Lock()
	defer p.pm.Unlock()

	delta, exists := p.keyValues[k]
	if !exists {
		return Errors.ChainGBErrorf(Errors.UpdateDeltaErr, Errors.DeltaUpdateNoDeltaErr, "for key %s", key)
	}
	if d.KeyGroup != delta.KeyGroup || d.Key != delta.Key {
		return Errors.ChainGBErrorf(Errors.UpdateDeltaErr, Errors.DeltaUpdateKeyErr, "for key %s", key)
	}

	// apply the user-provided mutation while holding p.pm
	apply(delta, d)

	if d.Version > p.maxVersion {
		p.maxVersion = d.Version
	}
	return nil
}

// GetAll simply returns the key value ClusterMap for the particular Participant with which we call the method on
func (p *Participant) GetAll() map[string]*Delta {
	return p.keyValues
}

// TODO Implement

func (p *Participant) Get(deltaKey string) (*Delta, error) {

	return nil, nil
}

//=======================================================
// Cluster Map Handling
//=======================================================

// initClusterMap returns a standard instance of a ClusterMap for the particular Participant passed in
// this function is mostly used to initialise our own servers ClusterMap as other nodes will have initialised their own maps
func initClusterMap(serverName string, seed *net.TCPAddr, participant *Participant) *ClusterMap {

	cm := &ClusterMap{
		&Seed{seedAddr: seed},
		make(map[string]*Participant),
		make([]string, 0, 4),
	}

	cm.participants[serverName] = participant
	cm.participantArray = append(cm.participantArray, participant.name)

	return cm

}

//-------------------------
//Updating cluster

// TODO This needs to be optimised and make sure we are not delaying any hot paths - currently doing too many things
// TODO May also want to rename to just addDeltaToMap? as we can receive a delta in a GOS_SYN as well..
func (s *GBServer) addGSADeltaToMap(delta *clusterDelta) error {

	// If we receive information about ourselves, we need to ignore as we are the only ones who should update information about ourselves
	// This does NOT mean we ignore any failure information about ourselves as every node carries failure deltas as Deltas within their maps
	// with the particular node name as the key
	// So while we ignore our own Participant information - we still can detect our own failure information within other Participants maps

	for uuid, d := range delta.delta {

		if uuid == s.ServerName {
			continue
		}

		// fetch participant under lock
		s.clusterMapLock.RLock()
		participant, exists := s.clusterMap.participants[uuid]
		s.clusterMapLock.RUnlock()

		if exists {

			// Here we have a participant with values we need to update for our map
			// Use the locking strategy to lock participant in order to update
			for k, v := range d.keyValues {

				if v.KeyGroup == FAILURE_DKG {
					// We are in our view of *Participant and received a failure key group from them which could be about
					// any node - we must check the key to understand which node
					if err := s.checkFailureGSA(uuid, v); err != nil {
						return Errors.ChainGBErrorf(Errors.AddGSAErr, err, "")
					}
				}

				// Initialise an update event
				de := &DeltaUpdateEvent{DeltaKey: v.Key, DeltaGroup: v.KeyGroup}

				// We get our own view of the participant in our map and update it
				err := participant.Update(v.KeyGroup, v.Key, v, func(toBeUpdated, by *Delta) {
					if by.Version > toBeUpdated.Version {
						de.PreviousVersion = toBeUpdated.Version
						de.PreviousValue = bytes.Clone(toBeUpdated.Value)

						// copy fields explicitly; clone the slice
						toBeUpdated.Version = by.Version
						toBeUpdated.KeyGroup = by.KeyGroup
						toBeUpdated.Key = by.Key
						toBeUpdated.Value = bytes.Clone(by.Value)

						de.CurrentVersion = toBeUpdated.Version
						de.CurrentValue = bytes.Clone(toBeUpdated.Value)

						if v.KeyGroup == CONFIG_DKG {
							// Need to think about how to handle error
							_ = s.updateSelfInfo(v) // locks a different participant (self), so safe
						}
					}
				})

				// We need to handle the error from the update delta method
				// If we receive no delta found GBError then we know to add as new delta
				// Any other error we know to return
				if err != nil {
					handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {

						for _, gbErr := range gbError {

							if gbErr.Code == Errors.DELTA_UPDATE_NO_DELTA_CODE && v.KeyGroup != CONFIG_DKG {
								participant.pm.Lock()
								participant.keyValues[k] = v
								participant.pm.Unlock()

								// Event call for new delta added
								s.DispatchEvent(Event{
									EventType: NewDeltaAdded,
									Time:      time.Now().Unix(),
									Payload: &DeltaAddedEvent{
										DeltaGroup: v.KeyGroup,
										DeltaKey:   v.Key,
										DeltaValue: bytes.Clone(v.Value),
									},
									Message: "New delta added", // Using this string in server test - careful if changing
								})
								return nil
							}
						}

						return gbError[len(gbError)-1]

					})

					if handledErr != nil {
						return Errors.ChainGBErrorf(Errors.AddGSAErr, err, "")
					}

				}

				// Event call for delta updated
				if v.Key != _HEARTBEAT_ {
					s.DispatchEvent(Event{
						DeltaUpdated,
						time.Now().Unix(),
						de,
						"Delta updated",
					})
				}

			}
		} else {

			// If here then we have a new participant to add
			err := s.addParticipantFromTmp(uuid, d)
			if err != nil {
				return Errors.ChainGBErrorf(Errors.AddGSAErr, err, "")
			}

		}
	}
	return nil
}

//=======================================================
// Participant Handling
//=======================================================

//Add/Remove Participant

// Thread safe
// addParticipantFromTmp takes a temporary participant as well as the key-values and adds them into our clusterMap
// A new failure delta is added to our self view to track this particular participants failure delta
func (s *GBServer) addParticipantFromTmp(name string, tmpP *tmpParticipant) error {

	// Step 1: Add participant to the participants map
	newParticipant := &Participant{
		name:      name,
		keyValues: make(map[string]*Delta), // Allocate a new map
		f:         newFailure(),
	}

	var maxV int64

	now := time.Now().Unix()

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

	s.clusterMapLock.Lock()
	s.clusterMap.participants[name] = newParticipant
	s.clusterMap.participantArray = append(s.clusterMap.participantArray, newParticipant.name)

	// Grab self quickly to add failure delta for this node to our map
	s.clusterMapLock.Unlock()

	err := s.addDeltaToSelfInfo(&Delta{KeyGroup: FAILURE_DKG, Key: name, Version: time.Now().Unix(), ValueType: D_UINT8_TYPE, Value: []byte{ALIVE}})
	if err != nil {
		return Errors.ChainGBErrorf(Errors.AddTmpPartErr, err, "")
	}

	// Dispatch new participant added event
	s.DispatchEvent(Event{
		NewParticipantAdded,
		now,
		&NewParticipantJoin{
			Name:       name,
			Time:       now,
			MaxVersion: maxV,
		},
		ParseEventEnumToString(NewParticipantAdded),
	})

	// Clear tmpParticipant references
	tmpP.keyValues = nil

	return nil
}

//=======================================================
// Discovery Phase
//=======================================================

// TODO Implement in future release
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
		name:      name,
		keyValues: make(map[string]*Delta),
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

func (s *GBServer) runDiscovery(ctx context.Context) error {

	seed, err := s.retrieveASeedConn(false) // We only retrieve a random seed if there is a lot of cluster load
	if err != nil {
		return err
	}

	err = s.conductDiscovery(ctx, seed)
	if err != nil {
		handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {
			for _, ge := range gbError {
				fmt.Printf("gb error = %v\n", ge)
			}
			return gbError[len(gbError)-1]
		})

		if errors.Is(handledErr, Errors.EmptyAddrMapNetworkErr) {
			fmt.Printf("%s - exiting discovery phase\n", s.PrettyName())
			s.discoveryPhase = false
		} else {
			fmt.Printf("discovery phase failed\n")
		}
	}

	fmt.Printf("discovery phase active - aborting gossip round\n")
	return nil

}

// TODO Implement in future release update
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
		return nil, Errors.ChainGBErrorf(Errors.DiscoveryReqErr, err, "")
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

	resp := conn.qProtoWithResponse(ctx, reqId, pay, false)

	r, err := conn.waitForResponseAndBlock(resp)
	if err != nil {
		fmt.Printf("GOT A DISCOVERY ERROR = %s\n", err.Error())
		return nil, Errors.ChainGBErrorf(Errors.DiscoveryReqErr, err, "")
	}

	fmt.Printf("response =============== %v\n", r)

	return r.msg, nil

}

func (s *GBServer) conductDiscovery(ctx context.Context, conn *gbClient) error {

	fmt.Printf("%s conducting discovery\n", s.name)

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
		return nil, Errors.ChainGBErrorf(Errors.DiscoveryReqErr, err, "")
	}

	if len(addrMap) == 0 {
		return nil, Errors.EmptyAddrMapErr
	}

	cereal, err := c.srv.serialiseDiscoveryAddrs(addrMap)
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.DiscoveryReqErr, err, "")
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
// generateDigest loops through our clusterMap and for each participant and produces a list of all participant names and
// their max versions. It then serializes and returns the bytes, size, and any errors.
func (s *GBServer) generateDigest() ([]byte, int, error) {

	// Can we be using read locks more efficiently here?
	s.clusterMapLock.RLock()
	cm := s.clusterMap
	partList := cm.participantArray
	partMap := cm.participants
	s.clusterMapLock.RUnlock()

	// first we estimate what a full digest could be and if we need to maybe only produce a digest chunk instead
	mtuEstimate := CEREAL_DIGEST_HEADER_SIZE + len(partMap)*32
	if mtuEstimate > MTU_DIGEST && len(partList) > 10 {
		var newPartArray []string
		subsetSize := 0

		for _, idx := range rand.Perm(len(partList)) {
			node := partList[idx]
			if _, ok := partMap[node]; !ok {
				continue
			}

			entrySize := 1 + len(node) + 8 // name + len prefix + version
			if subsetSize+entrySize > MTU_DIGEST {
				break
			}

			newPartArray = append(newPartArray, node)
			subsetSize += entrySize
		}

		cereal, err := s.serialiseClusterDigestWithArray(newPartArray, subsetSize)
		if err != nil {
			return nil, 0, Errors.ChainGBErrorf(Errors.GenerateDigestErr, nil, "%s", err)
		}
		return cereal, subsetSize, nil
	}

	return s.serialiseClusterDigest()
}

// modifyDigest is a helper function for removing the CLRF at the end of a digest if we have received a GOS_SYN with
// a digest + delta we only need the CLRF at the end of the delta. We return the modified bytes, the size and a fmt error
// as this is a ground level method
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

//----------------------------------------
// Send Digest in GOSS_SYN - Stage 1

// sendDigest takes the parent context (gossip round context) and makes a child context which will have a baseline timeout
// if sendDigest reaches its deadline, the remaining parent context will be used as the timeout for an indirect probe.
// sendDigest compiles a digest of (node_name:[key:version]) to send to a receiving node in which a delta can be built.
func (s *GBServer) sendDigest(ctx context.Context, conn *gbClient) (responsePayload, error) {

	// We create a child context which will be the gossipRoundTimeout not including the extra timeout reserved for indirect probing
	// If we reach the deadline, we fall out, and use the parent context and remainder timeout as the indirect probe
	childCtx, cancel := context.WithTimeout(ctx, s.fail.gossipTimeout*time.Millisecond)
	defer cancel()

	// Generate the digest
	digest, _, err := s.generateDigest()
	if err != nil {
		return responsePayload{}, err
	}

	// Acquire request ID
	reqID, err := s.acquireReqID()
	if err != nil {
		return responsePayload{}, Errors.ChainGBErrorf(Errors.GenerateDigestErr, err, "")
	}

	// Construct the packet
	header := constructNodeHeader(1, GOSS_SYN, reqID, uint16(0), uint16(len(digest)), NODE_HEADER_SIZE_V1)
	packet := &nodePacket{
		header,
		digest,
	}
	cereal, gbErr := packet.serialize()
	if gbErr != nil {
		return responsePayload{}, Errors.ChainGBErrorf(Errors.GenerateDigestErr, err, "")
	}

	select {
	case <-ctx.Done():
		return responsePayload{}, Errors.ChainGBErrorf(Errors.GenerateDigestErr, Errors.ContextErr, "context: %s", ctx.Err())
	default:

	}

	// Send the digest and wait for a response
	resp := conn.qProtoWithResponse(childCtx, reqID, cereal, false)

	r, err := conn.waitForResponseAndBlock(resp)
	if err != nil {
		if childCtx.Err() != nil {
			return responsePayload{}, Errors.ChainGBErrorf(Errors.GenerateDigestErr, err, "child context: %s", ctx.Err())
		}
		return responsePayload{}, Errors.ChainGBErrorf(Errors.GenerateDigestErr, err, "")
	}

	return r, nil

}

//=======================================================
// GOSS_SYN_ACK Prep
//=======================================================

// generateParticipantHeap initialises a priority heap to fill with participants which have available deltas where the max
// versions are above the provided versions in the received digest
func (s *GBServer) generateParticipantHeap(sender string, digest *fullDigest) (ph participantHeap, err error) {

	// Here we are not looking at participants we are missing for us - that will be sent to us when we send our digest
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
		return nil, Errors.ChainGBErrorf(Errors.EmptyParticipantHeapErr, nil, "for sender %s", sender)
	}

	return partQueue, nil

}

// TODO Returns no errors - need to fix
// buildDelta takes a participantHeap and looks at what versions we have that are above the max version for each participant
// we prioritise the most outdated participants on the heap as per scuttle depth. We return selected deltas, size and any errors
func (s *GBServer) buildDelta(ph *participantHeap, remaining int) (finalDelta map[string][]Delta, size int, err error) {

	// Need to go through each participant in the heap - add each delta to a heap order it - pop each delta
	// and get it's size + node + metadata if within bounds, we can either serialise here OR
	// we can store selected deltas in a map against node keys and return them along with a size ready to be passed
	// to serialiser

	s.clusterMapLock.RLock()
	cm := s.clusterMap
	s.clusterMapLock.RUnlock()

	sizeOfDelta := 0

	selectedDeltas := make(map[string][]Delta)

	for ph.Len() > 0 && sizeOfDelta < remaining {

		phEntry := heap.Pop(ph).(*participantQueue)
		participant := cm.participants[phEntry.name]

		if participant.maxVersion <= phEntry.peerMaxVersion {
			continue
		}

		// Make a delta heap for each participant
		// Are we being inefficient here by pushing all deltas onto the heap everytime?
		dh := make(deltaHeap, 0, len(participant.keyValues))
		for _, delta := range participant.keyValues {

			size := DELTA_META_SIZE + len(delta.KeyGroup) + len(delta.Key) + len(delta.Value)

			if delta.Version > phEntry.peerMaxVersion {
				//s.logger.Info("adding delta", "key", delta.Key)
				heap.Push(&dh, &deltaQueue{
					delta:   delta,
					version: delta.Version,
					size:    size,
				})
			}
		}
		heap.Init(&dh)

		// We are to send the highest version deltas, and we fit as many deltas in based on remaining space.

		// First add the participant size to the sizeOfDelta
		sizeOfDelta += 1 + len(participant.name) + 2 // 1 byte for name length + name + size of delta key-values

		deltaList := make([]Delta, 0, len(dh))

		// Make selected delta list here and populate

		for dh.Len() > 0 {

			d := heap.Pop(&dh).(*deltaQueue)

			//s.logger.Info("adding delta", "value", d.delta.Value)

			if d.size+sizeOfDelta > remaining {
				sd := size + sizeOfDelta
				s.logger.Warn("broke size", "size", sd, "name", s.PrettyName(), "dropping", d.delta.Value)
				break
			}

			sizeOfDelta += d.size

			deltaList = append(deltaList, *d.delta)

		}
		if len(deltaList) > 0 {
			selectedDeltas[participant.name] = deltaList
		}
	}
	return selectedDeltas, sizeOfDelta, nil
}

// prepareGossSynAck takes the digest and generates our own digest. It then builds a delta so send back if we have one available
// the return is a serialised digest and a delta if we have none
func (s *GBServer) prepareGossSynAck(sender string, digest *fullDigest) ([]byte, error) {

	// Prepare our own digest first as we need to know if digest reaches its cap, so we know how much space we have left for the Delta
	d, _, err := s.generateDigest()
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.SendGSAErr, err, "")
	}

	// Compare here - Will need to take a remaining size left over from generating our digest
	partQueue, err := s.generateParticipantHeap(sender, digest)
	if err != nil {

		handledErr := Errors.HandleError(err, func(gbError []*Errors.GBError) error {
			for _, gbError := range gbError {
				if gbError.Code == Errors.EMPTY_PARTICIPANT_HEAP_CODE {
					return gbError
				}
			}
			return nil
		})
		if handledErr != nil {
			return d, nil
		}
		return nil, nil

	}

	//Modify digest if no error
	newD, newSize, err := s.modifyDigest(d)
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.SendGSAErr, nil, "%s", err)
	}

	remaining := int(DEFAULT_MAX_GSA) - newSize

	// Populate delta queues and build selected deltas
	selectedDeltas, deltaSize, err := s.buildDelta(&partQueue, remaining)
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.SendGSAErr, err, "")
	}

	// Serialise
	cereal, err := s.serialiseGSA(newD, selectedDeltas, deltaSize)

	// Return
	return cereal, nil
}

//-----------------------------------------------------
// Send Digest And Delta in GOSS_SYN_ACK - Stage 2

// sendGossSynAck prepares the acknowledgement to send back after receiving a goss_syn, it sends a digest and a delta if we
// have one available. It then waits for a response async and processes the goss_syn_ack
func (c *gbClient) sendGossSynAck(sender string, digest *fullDigest) error {

	reqID := c.ph.reqID

	// serialise and send?
	gsa, err := c.srv.prepareGossSynAck(sender, digest)
	if err != nil {
		return err
	}

	respID, err := c.srv.acquireReqID()
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendGSAErr, err, "")
	}

	pay, err := prepareRequest(gsa, 1, GOSS_SYN_ACK, reqID, respID)
	if err != nil {
		return Errors.ChainGBErrorf(Errors.SendGSAErr, err, "")
	}

	ctx, cancel := context.WithTimeout(c.srv.ServerContext, 1*time.Second)

	resp := c.qProtoWithResponse(ctx, respID, pay, false)

	c.waitForResponseAsync(resp, func(delta responsePayload, err error) {

		if err != nil {
			// Need to handle the error but for now just return
			cancel()
			return
		}

		// TODO Right now we're not handling these errors
		// Off-load heavy work from the hot path
		go func() {
			defer cancel() // signal caller after merge is done

			cd, e := deserialiseDelta(delta.msg)
			if e != nil {
				return
			}

			// 2. Merge into server state (must be thread-safe!)
			if e := c.srv.addGSADeltaToMap(cd); e != nil {
				c.srv.logger.Info("received error when adding goss_syn_ack to map",
					slog.String("error", e.Error()),
					slog.String("sender", sender),
				)
				return
			}
		}()

	})

	return nil
}

//=======================================================
// GOSS_ACK Prep
//=======================================================

// prepareACK takes the final digest from the goss_syn_ack response and either returns a serialised delta or nil if it
// nothing to send
func (s *GBServer) prepareACK(sender string, fd *fullDigest) ([]byte, error) {

	// Compare here - Will need to take a remaining size left over from generating our digest
	if fd == nil {
		return nil, Errors.ChainGBErrorf(Errors.GossAckErr, Errors.NoDigestErr, "")
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
			return nil, Errors.ChainGBErrorf(Errors.GossAckErr, Errors.EmptyParticipantHeapErr, "")
		}
		return nil, nil

	}

	remaining := int(DEFAULT_MAX_GSA)

	// Populate delta queues and build selected deltas
	selectedDeltas, deltaSize, err := s.buildDelta(&partQueue, remaining)
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.GossAckErr, err, "")
	}

	delta, err := s.serialiseACKDelta(selectedDeltas, deltaSize)
	if err != nil {
		return nil, Errors.ChainGBErrorf(Errors.GossAckErr, err, "")
	}

	return delta, nil

}

// TODO possibly split into another file?
//========================================================================================
// GOSSIP
//========================================================================================

//=======================================================
// Gossip Signalling + Process
//=======================================================

//----------------
//Gossip Signalling

// Thread safe
// checkGossipCondition checks gossip flags and sends bool on control channel to either pause or start gossip
func (s *GBServer) checkGossipCondition() {
	nodes := atomic.LoadInt64(&s.numNodeConnections)

	if nodes >= 1 && !s.flags.isSet(GOSSIP_SIGNALLED) {
		s.serverLock.Lock()
		s.flags.set(GOSSIP_SIGNALLED)
		s.serverLock.Unlock()
		s.gossip.gossipControlChannel <- true
		s.gossip.gossSignal.Broadcast()

	} else if nodes < 1 && s.flags.isSet(GOSSIP_SIGNALLED) {
		s.gossip.gossipControlChannel <- false
		s.serverLock.Lock()
		s.flags.clear(GOSSIP_SIGNALLED)
		s.serverLock.Unlock()
	}

}

//------------------------------
//Gossip Control

// gossipProcess is the main loop for gossip. It waits on a sync.Cond broadcast signal to start gossip rounds
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
		s.gossip.gossSignal.L.Lock()

		if s.ServerContext.Err() != nil {
			//s.endGossip()
			s.gossip.gossMu.Unlock()
			return
		}

		// Wait for gossipOK to become true, or until serverContext is canceled.
		if !s.gossip.gossipOK || !s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {
			s.gossip.gossSignal.Wait() // Wait until gossipOK becomes true
			s.gossip.gossipPaused = false
		}

		if s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {
			s.gossip.gossMu.Unlock()
			return
		}

		s.gossip.gossSignal.L.Unlock()

		// now safe to call
		ok := s.startGossipProcess()

		// optionally re-acquire if needed
		s.gossip.gossSignal.L.Lock()
		s.gossip.gossipOK = ok
		s.gossip.gossSignal.L.Unlock()

	}
}

//----------------
//Gossip Check

// tryStartGossip is a semaphore check to see if we are currently gossiping. It blocks future calls to start gossip rounds
// if the semaphore is held
func (s *GBServer) tryStartGossip() bool {
	// Check if shutting down or context is canceled before attempting gossip
	if s.flags.isSet(SHUTTING_DOWN) || s.ServerContext.Err() != nil {
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

// endGossip releases the semaphore so tryStartGossip doesn't stop another round
func (s *GBServer) endGossip() {
	select {
	case <-s.gossip.gossipSemaphore:
	default:
	}
}

//----------------
//Gossip Process

func (s *GBServer) startGossipProcess() bool {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Exit immediately if the server context is already canceled
	if s.ServerContext.Err() != nil {
		return false
	}
	for {
		select {
		case <-s.ServerContext.Done():
			s.gossip.gossWg.Wait() // Wait for the rounds to finish
			s.endGossip()          // Ensure state is reset
			return false
		case <-ticker.C:

			// Check if context cancellation occurred
			if s.ServerContext.Err() != nil || s.flags.isSet(SHUTTING_DOWN) {
				s.gossip.gossWg.Wait()
				s.endGossip()
				return false
			}

			// Attempt to start a new gossip round
			if !s.tryStartGossip() {

				if s.flags.isSet(SHUTTING_DOWN) {
					return false
				}

				fmt.Printf("%s - Skipping gossip round because a round is already active\n", s.PrettyName())

				continue
			}

			s.gossip.gossWg.Add(1)
			s.startGossipRound()

		case gossipState := <-s.gossip.gossipControlChannel:
			if !gossipState {
				s.logger.Info("gossip stopped???")
				// If gossipControlChannel sends 'false', stop the gossiping process
				// Pause the phi process also
				//s.phi.phiControl <- false
				s.gossip.gossWg.Wait()
				s.endGossip()
				s.gossip.gossipPaused = true
				return false
			}
		}
	}
}

//=======================================================
// Gossip Round
//=======================================================

// startGossipRound is the main gossip round which selects random participants to gossip with and starts rounds. It waits
// for all rounds to finish before returning or before timeout
func (s *GBServer) startGossipRound() {

	start := time.Now()

	var indexes []int

	defer func() {
		duration := time.Since(start)

		s.logger.Info("gossip round complete",
			slog.String("node", s.PrettyName()),
			slog.Duration("duration", duration),
			slog.Int("peers_contacted", len(indexes)),
			slog.Int("active participants", len(s.clusterMap.participantArray)),
		)

		s.endGossip()
		s.gossip.gossWg.Done()
	}()

	// TODO Implement in future release
	//for discoveryPhase we want to be quick here and not hold up the gossip round - so we conduct discovery and exit
	//log.Printf("are we in a discovery phase ====== %v", s.discoveryPhase)
	//if s.discoveryPhase {
	//	go func() {
	//		err := s.runDiscovery(ctx)
	//		if err != nil {
	//			fmt.Printf("%s - Gossip discovery failed: %v\n", s.PrettyName(), err)
	//		}
	//	}()
	//}

	s.configLock.RLock()
	ns := s.gbClusterConfig.GetNodeSelection()
	s.configLock.RUnlock()

	pl := len(s.clusterMap.participantArray)
	if int(ns) > pl-1 {
		ns = 1
	}

	s.clusterMapLock.RLock()
	partList := s.clusterMap.participantArray
	s.clusterMapLock.RUnlock()

	indexes, err := generateRandomParticipantIndexesForGossip(partList, int(ns), &s.notToGossipNodeStore, "")
	if err != nil {
		// TODO Need to add error event as its internal system error
		return
	}

	var wg sync.WaitGroup

	for _, idx := range indexes {
		nodeID := partList[idx]

		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			nodeCtx, cancel := context.WithTimeout(s.ServerContext, s.fail.maxGossipRoundTimeout*time.Millisecond)
			defer cancel()

			s.gossipWithNode(nodeCtx, node)

			if nodeCtx.Err() != nil {

			}

		}(nodeID)
	}

	// Wait for completion or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-s.ServerContext.Done():
		return
	case <-done:
		// Completed successfully
	}

	_ = s.updateHeartBeat(time.Now().Unix()) // Mark end of round

}

//TODO Need to decide how to propagate errors

// gossipWithNode is the singular gossip round between our server and a node. It gets the connection, and conducts each stage of the
// gossip process
func (s *GBServer) gossipWithNode(ctx context.Context, node string) {

	if s.flags.isSet(SHUTTING_DOWN) {
		return
	}

	//------------- Dial Check -------------//

	conn, exists, err := s.getNodeConnFromStore(node)
	if err == nil && !exists {
		go func() {
			dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := s.connectToNodeInMap(dialCtx, node)
			if err != nil {
				// TODO Need to make GBError and add to an error event
			}
		}()
		return
	}
	if err != nil {
		return
	}

	//------------- GOSS_SYN Stage 1 -------------//

	// Stage 1: Send Digest
	resp, err := s.sendDigest(ctx, conn)
	if err != nil {
		handledErr := Errors.HandleError(err, func(gbErrors []*Errors.GBError) error {

			for _, gbError := range gbErrors {
				if gbError.Code == Errors.CONTEXT_ERROR_CODE {
					probeErr := s.handleIndirectProbe(ctx, node)
					if probeErr != nil {
						// May want to take action here?
						s.logger.Error(probeErr.Error())
						return nil
					}
					return nil
				}
			}

			return err
		})
		if handledErr != nil {
			s.logger.Error("error gossiping with node", slog.String("node", node),
				slog.String("error", handledErr.Error()))
			return
		}
		return
	}

	//------------- GOSS_SYN_ACK Stage 2 -------------//

	// Stage 2: Handle goss_syn_ack response
	sender, fdValue, cdValue, err := deserialiseGSA(resp.msg)
	if err == nil && cdValue != nil {
		_ = s.addGSADeltaToMap(cdValue)
	}

	//------------- GOSS_ACK Stage 3 -------------//

	// Stage 3: Send goss_ack
	ack, err := s.prepareACK(sender, fdValue)
	if err != nil || ack == nil {
		conn.sendErrResp(uint16(0), resp.respID, Errors.DeltaUpdateNoDeltaErr.Net())
		return
	}

	pay, err := prepareRequest(ack, 1, GOSS_ACK, uint16(0), resp.respID)
	if err == nil {
		conn.mu.Lock()
		conn.enqueueProto(pay)
		conn.mu.Unlock()
	}

	// Complete
	return

}
