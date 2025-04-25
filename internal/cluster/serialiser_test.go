package Cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func TestDiscoveryRequestSerialiser(t *testing.T) {
	tests := []struct {
		name          string
		clusterMap    ClusterMap
		expectedError bool
		expectedLen   int
	}{
		{
			name: "Normal Cluster Map",
			clusterMap: func() ClusterMap {
				cm := ClusterMap{participants: make(map[string]*Participant, 5)}
				for i := 0; i < 5; i++ {
					partName := fmt.Sprintf("node-%d", i)
					part := &Participant{
						name:       partName,
						keyValues:  make(map[string]*Delta, 1),
						maxVersion: 0,
					}
					part.keyValues[_ADDRESS_] = &Delta{
						Key:       _ADDRESS_,
						Version:   0,
						ValueType: ADDR_V,
						Value:     []byte("127.0.0.1"),
					}
					cm.participants[partName] = part
				}
				return cm
			}(),
			expectedError: false,
			expectedLen:   6,
		},
		{
			name: "1 Node Missing Address",
			clusterMap: func() ClusterMap {
				cm := ClusterMap{participants: make(map[string]*Participant, 5)}
				for i := 0; i < 5; i++ {
					partName := fmt.Sprintf("node-%d", i)
					part := &Participant{
						name:       partName,
						keyValues:  make(map[string]*Delta, 1),
						maxVersion: 0,
					}
					if i == 2 {
						part.keyValues[_NODE_CONNS_] = &Delta{
							Key:       _NODE_CONNS_,
							Version:   0,
							ValueType: NUM_NODE_CONN_V,
							Value:     []byte{0},
						}
					} else {
						part.keyValues[_ADDRESS_] = &Delta{
							Key:       _ADDRESS_,
							Version:   0,
							ValueType: ADDR_V,
							Value:     []byte("127.0.0.1"),
						}
					}
					cm.participants[partName] = part
				}
				return cm
			}(),
			expectedError: false,
			expectedLen:   5,
		},
		{
			name: "No addresses",
			clusterMap: func() ClusterMap {
				cm := ClusterMap{participants: make(map[string]*Participant, 5)}
				for i := 0; i < 5; i++ {
					partName := fmt.Sprintf("node-%d", i)
					part := &Participant{
						name:       partName,
						keyValues:  make(map[string]*Delta, 1),
						maxVersion: 0,
					}
					part.keyValues[_NODE_CONNS_] = &Delta{
						Key:       _NODE_CONNS_,
						Version:   0,
						ValueType: NUM_NODE_CONN_V,
						Value:     []byte{0},
					}
					cm.participants[partName] = part
				}
				return cm
			}(),
			expectedError: true,
			expectedLen:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gbs := &GBServer{
				ServerName: "main-server",
				clusterMap: tt.clusterMap,
			}

			// Step 1: Get Known Address Nodes
			knownNodes, err := gbs.getKnownAddressNodes()
			if (err != nil) != tt.expectedError {
				t.Fatalf("GetKnownAddressNodes() error = %v, expected error %v", err, tt.expectedError)
			}
			if tt.expectedError {
				// If an error was expected, stop the test here.
				return
			}

			// Step 2: Serialize Known Nodes
			cereal, err := gbs.serialiseKnownAddressNodes(knownNodes)
			if err != nil {
				t.Fatalf("Error serialising known addresses: %v", err)
			}

			// Step 3: Deserialize Known Nodes
			result, err := deserialiseKnownAddressNodes(cereal)
			if err != nil {
				t.Fatalf("Error deserialising known addresses: %v", err)
			}

			// Step 4: Validate Results
			if result[0] != gbs.ServerName {
				t.Errorf("Expected result[0] to be %s, got %s", gbs.ServerName, result[0])
			}

			if len(result) != tt.expectedLen {
				t.Errorf("Expected length to be %d, got %d", tt.expectedLen, len(result))
			}

		})
	}
}

func TestSerialiseDiscoveryRequest(t *testing.T) {

	gbs := GenerateDefaultTestServer("main-server", addressTestingKVs, 5)

	knownAddr := make([]string, 2)

	for i := 0; i < 2; i++ {
		name := gbs.clusterMap.participantArray[i]
		knownAddr[i] = name
	}

	data, err := gbs.serialiseKnownAddressNodes(knownAddr)
	if err != nil {
		t.Fatalf("Error serialising known addresses: %v", err)
	}

	parts, err := deserialiseKnownAddressNodes(data)
	if err != nil {
		t.Fatalf("Error deserialising known addresses: %v", err)
	}

	log.Printf("data = %+s", parts)

	// First element is the sender
	//gbs.serialiseDiscoveryResponse(parts)

	//log

}

func TestDiscoveryResponse(t *testing.T) {

	gbs := GenerateDefaultTestServer("main-server", multipleAddressTestingKVs, 5)

	knownAddr := make(map[string][]string, 2)

	for i := 0; i < 2; i++ {
		name := gbs.clusterMap.participantArray[i]
		knownAddr[name] = make([]string, 2)
		knownAddr[name][0] = gbs.clusterMap.participants[name].keyValues[_ADDRESS_].Key
		knownAddr[name][1] = gbs.clusterMap.participants[name].keyValues["CLOUD"].Key
	}

	log.Printf("data = %+s", knownAddr)

	data, err := gbs.serialiseDiscoveryAddrs(knownAddr)
	if err != nil {
		t.Fatalf("Error serialising known addresses: %v", err)
	}

	log.Printf("addresses = %+v", data)

	addrMap, err := deserialiseDiscovery(data)
	if err != nil {
		t.Fatalf("Error deserialising addresses: %v", err)
	}

	for key, value := range addrMap.dv {
		log.Printf("addr = %s%+v", key, value.addr)
	}

}

func TestDiscoveryResponseTable(t *testing.T) {

	gbs := GenerateDefaultTestServer("main-server", multipleAddressTestingKVs, 5)

	tests := []struct {
		name           string
		addrMap        map[string][]string
		checkNodeCount int
		checkAddrMap   map[string]map[string]string
	}{
		{
			name: "simple tcp addr map",
			addrMap: map[string][]string{
				gbs.ServerName: []string{
					_ADDRESS_,
				},
			},
			checkNodeCount: 1,
			checkAddrMap: map[string]map[string]string{
				gbs.ServerName: {
					_ADDRESS_: "127.0.0.1",
				},
			},
		},

		{
			name: "multi addr map",
			addrMap: map[string][]string{
				gbs.ServerName: []string{
					_ADDRESS_,
					"CLOUD",
					"DNS",
				},
				gbs.clusterMap.participantArray[1]: []string{
					_ADDRESS_,
					"CLOUD",
				},
				gbs.clusterMap.participantArray[2]: []string{
					"DNS",
				},
			},
			checkNodeCount: 3,
			checkAddrMap: map[string]map[string]string{
				gbs.ServerName: {
					_ADDRESS_: "127.0.0.1",
					"CLOUD":   "137.184.248.0",
					"DNS":     "example.com",
				},
				gbs.clusterMap.participantArray[1]: {
					_ADDRESS_: "127.0.0.1",
					"CLOUD":   "137.184.248.0",
				},
				gbs.clusterMap.participantArray[2]: {
					"DNS": "example.com",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cereal, err := gbs.serialiseDiscoveryAddrs(tt.addrMap)
			if err != nil {
				t.Fatalf("Error serialising addresses: %v", err)
			}

			addrResultMap, err := deserialiseDiscovery(cereal)
			if err != nil {
				t.Fatalf("Error deserialising addresses: %v", err)
			}

			// Make asserts

			if len(addrResultMap.dv) != tt.checkNodeCount {
				t.Errorf("node count mismatch: got %d, want %d", len(addrResultMap.dv), tt.checkNodeCount)
			}

			for name, value := range addrResultMap.dv {
				if _, exists := tt.checkAddrMap[name]; !exists {
					t.Errorf("node name does not exist: %s", name)
				}
				log.Printf("got name %s", name)
				for key, addr := range value.addr {
					if key, exists := tt.checkAddrMap[name][key]; !exists {
						t.Errorf("address not found for key: %s", key)
					}
					log.Printf("got address key %s - value: %s", key, addr)
					if addr != tt.checkAddrMap[name][key] {
						t.Errorf("address mismatch: got %s, want %s", addr, tt.checkAddrMap[name][key])
					}
				}
			}

		})
	}

}

func TestSerialiseDigest(t *testing.T) {

	// Create keyValues with PBDelta messages
	keyValues := map[string]*Delta{
		"key6":  {ValueType: INTERNAL_D, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"key7":  {ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("A")},
		"key8":  {ValueType: INTERNAL_D, Version: 1640995206, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"key9":  {ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("😃 Emoji support test.")},
		"key10": {ValueType: INTERNAL_D, Version: 1640995208, Value: []byte("Another simple string.")},
	}

	// Mock server setup
	gbs := &GBServer{
		clusterMap: ClusterMap{
			participants: make(map[string]*Participant, 5), // 5 Participants for the test
		},
	}

	// Add 5 mock participants to the cluster
	for i := 1; i <= 5; i++ {
		participantName := fmt.Sprintf("node-test%d", i)

		// Create a participant
		participant := &Participant{
			name:       participantName,
			keyValues:  make(map[string]*Delta),
			maxVersion: 0,
		}

		var maxVersion int64
		maxVersion = 0

		// Populate participant's keyValues
		for key, delta := range keyValues {

			participant.keyValues[key] = delta

			if delta.Version > maxVersion {
				maxVersion = delta.Version
			}

		}

		participant.maxVersion = maxVersion

		// Add participant to the ClusterMap
		gbs.clusterMap.participants[participantName] = participant

	}

	cereal, _, err := gbs.serialiseClusterDigest()
	if err != nil {
		log.Println("error ", err)
	}

	log.Printf("serialised digest = %v", cereal)

	_, digest, err := deSerialiseDigest(cereal)

	for _, value := range *digest {
		log.Printf("%v:%v", value.nodeName, value.maxVersion)
	}

}

func TestSerialiseDigestWithSubsetArray(t *testing.T) {

	keyValues := map[string]*Delta{
		"key6":  {ValueType: INTERNAL_D, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"key7":  {ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("A")},
		"key8":  {ValueType: INTERNAL_D, Version: 1640995206, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"key9":  {ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("😃 Emoji support test.")},
		"key10": {ValueType: INTERNAL_D, Version: 1640995208, Value: []byte("Another simple string.")},
	}

	// Mock server setup
	gbs := &GBServer{
		clusterMap: ClusterMap{
			participants: make(map[string]*Participant, 5), // 5 Participants for the test
		},
	}

	// Need to create multiple participants to be used as a subset
	// Add 5 mock participants to the cluster
	for i := 1; i <= 10; i++ {
		participantName := fmt.Sprintf("node-test%d", i)

		// Create a participant
		participant := &Participant{
			name:       participantName,
			keyValues:  make(map[string]*Delta),
			maxVersion: 0,
		}

		var maxVersion int64
		maxVersion = 0

		// Populate participant's keyValues
		for key, delta := range keyValues {

			participant.keyValues[key] = delta

			if delta.Version > maxVersion {
				maxVersion = delta.Version
			}

		}

		participant.maxVersion = maxVersion

		// Add participant to the ClusterMap + array
		gbs.clusterMap.participants[participantName] = participant
		gbs.clusterMap.participantArray = append(gbs.clusterMap.participantArray, participantName)

	}

	// Create subset array
	subsetArray := make([]string, 3)
	selectedNodes := make(map[string]struct{})
	subsetSize := 0

	for i := 0; i < 3; i++ {
		randNum := rand.Intn(len(gbs.clusterMap.participants))
		node := gbs.clusterMap.participantArray[randNum]
		if _, exists := selectedNodes[node]; exists {
			continue // Skip duplicates
		}
		selectedNodes[node] = struct{}{}
		subsetArray[i] = node

		subsetSize += 1 + len(node) + 8
	}

	log.Printf("subset array = %+v", subsetArray)

	newDigest, err := gbs.serialiseClusterDigestWithArray(subsetArray, subsetSize)
	if err != nil {
		log.Println("error ", err)
	}

	_, digest, err := deSerialiseDigest(newDigest)
	if err != nil {
		log.Println("error ", err)
	}

	for _, value := range *digest {
		log.Printf("%v:%v", value.nodeName, value.maxVersion)
	}

}

func BenchmarkJSONSerialization(b *testing.B) {
	keyValues := map[string]*Delta{
		"key1":  &Delta{ValueType: INTERNAL_D, Version: 1640995200, Value: []byte("hello world")},
		"key2":  &Delta{ValueType: INTERNAL_D, Version: 1640995200, Value: []byte("I've known adventures, seen places you people will never see, I've been Offworld and back... frontiers!")},
		"key3":  &Delta{ValueType: INTERNAL_D, Version: 1640995201, Value: []byte("short")},
		"key4":  &Delta{ValueType: INTERNAL_D, Version: 1640995202, Value: []byte("This is a slightly longer string to test serialization.")},
		"key5":  &Delta{ValueType: INTERNAL_D, Version: 1640995203, Value: []byte("1234567890")},
		"key6":  &Delta{ValueType: INTERNAL_D, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
		"key7":  &Delta{ValueType: INTERNAL_D, Version: 1640995205, Value: []byte("A")},
		"key8":  &Delta{ValueType: INTERNAL_D, Version: 1640995206, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
		"key9":  &Delta{ValueType: INTERNAL_D, Version: 1640995207, Value: []byte("😃 Emoji support test.")},
		"key10": &Delta{ValueType: INTERNAL_D, Version: 1640995208, Value: []byte("Another simple string.")},
		"key11": &Delta{ValueType: INTERNAL_D, Version: 1640995209, Value: []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")},
		"key12": &Delta{ValueType: INTERNAL_D, Version: 1640995210, Value: []byte("abcdefghijklmnopqrstuvwxyz")},
		"key13": &Delta{ValueType: INTERNAL_D, Version: 1640995211, Value: []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")},
		"key14": &Delta{ValueType: INTERNAL_D, Version: 1640995212, Value: []byte("Yet another string, this one a bit longer than the previous.")},
		"key15": &Delta{ValueType: INTERNAL_D, Version: 1640995213, Value: []byte("Small string.")},
		"key16": &Delta{ValueType: INTERNAL_D, Version: 1640995214, Value: []byte("A moderately sized string for testing.")},
		"key17": &Delta{ValueType: INTERNAL_D, Version: 1640995215, Value: []byte("Let's see how this performs with multiple keys and varying sizes.")},
		"key18": &Delta{ValueType: INTERNAL_D, Version: 1640995216, Value: []byte("This is one of the longest strings in this set, specifically designed to test the serialization performance and buffer handling.")},
		"key19": &Delta{ValueType: INTERNAL_D, Version: 1640995217, Value: []byte("Medium length string for benchmarking purposes.")},
		"key20": &Delta{ValueType: INTERNAL_D, Version: 1640995218, Value: []byte("Final key-value pair.")},
	}

	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(keyValues)
	}
}

func TestGSASerialisation(t *testing.T) {

	gbs := GenerateDefaultTestServer("main-server", keyValues1, 5)

	sizeOfDelta := 0
	selectedDeltas := make(map[string][]*Delta)

	for _, value := range gbs.clusterMap.participants {

		sizeOfDelta += 1 + len(value.name) + 2

		deltaList := make([]*Delta, 0)

		// Only want to add 2 deltas each participant
		i := 0
		for _, v := range value.keyValues {

			if i == 2 {
				continue
			}

			size := DELTA_META_SIZE + len(v.KeyGroup) + len(v.Key) + len(v.Value)
			sizeOfDelta += size

			deltaList = append(deltaList, v)

			i++

		}

		if len(deltaList) > 0 {
			selectedDeltas[value.name] = deltaList
		}
	}

	gsa, err := gbs.serialiseACKDelta(selectedDeltas, sizeOfDelta)
	if err != nil {
		t.Fatalf("Failed to serialise GSA: %v", err)
	}

	cd, err := deserialiseDelta(gsa)
	if err != nil {
		t.Fatalf("Failed to deserialise GSA: %v", err)
	}

	for n, fdValue := range cd.delta {
		log.Printf("delta check")
		for k, v := range fdValue.keyValues {
			log.Printf("%s --> key %s = %+v", n, k, v)
		}
	}

	// TODO In the serialiser there is duplication - probably because of the delta key and delta.key

	//for k, v := range gbs.clusterMap.participants[gbs.ServerName].keyValues {
	//	log.Printf("key %s = %+v", k, v)
	//}

}

func BenchmarkMapIteration(b *testing.B) {
	m := make(map[string]int)
	for i := 0; i < 100; i++ {
		m[fmt.Sprintf("participant%d", i)] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k, v := range m {
			_ = k
			_ = v
		}
	}
}

func BenchmarkSliceIteration(b *testing.B) {
	s := make([]string, 100)
	for i := 0; i < 100; i++ {
		s[i] = fmt.Sprintf("participant%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range s {
			_ = v
		}
	}
}
