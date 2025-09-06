package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/version"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func startPprofServer(ip, port string) {
	go func() {
		addr := fmt.Sprintf("%s:%s", ip, port)
		fmt.Printf("[pprof] HTTP server running at: http://%s/debug/pprof/\n", addr)

		fmt.Println("[pprof] Available endpoints:")
		fmt.Printf("  📈 CPU Profile:        http://%s/debug/pprof/profile?seconds=30\n", addr)
		fmt.Printf("  🧠 Heap Profile:       http://%s/debug/pprof/heap\n", addr)
		fmt.Printf("  🕳️ Block Profile:      http://%s/debug/pprof/block\n", addr)
		fmt.Printf("  🔒 Mutex Profile:      http://%s/debug/pprof/mutex\n", addr)
		fmt.Printf("  🔍 Goroutines:         http://%s/debug/pprof/goroutine?debug=2\n", addr)
		fmt.Printf("  📉 Execution Trace:    http://%s/debug/pprof/trace?seconds=5\n", addr)

		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Printf("[pprof] failed to start: %v\n", err)
		}
	}()
}

func init() {
	runtime.SetBlockProfileRate(1) // Sample every blocking event
}

type grTracking struct {
	numRoutines  int64
	index        int64
	trackingFlag atomic.Value
	grWg         *sync.WaitGroup
	routineMap   sync.Map
}

func (g *grTracking) startGoRoutine(serverName, name string, f func()) {
	// Add to the WaitGroup to track the goroutine
	g.grWg.Add(1)

	// Check if tracking is enabled
	if tracking, ok := g.trackingFlag.Load().(bool); ok && tracking {
		// Generate a new unique ID for the goroutine
		id := atomic.AddInt64(&g.index, 1)
		atomic.AddInt64(&g.numRoutines, 1)

		// Store the routine's information in the map
		g.routineMap.Store(id, name)

		// Launch the goroutine
		go func() {
			defer g.grWg.Done()
			defer func() {

				// TODO Think about if we actually want to recover - we should not be having routine problems
				// Recover from any panic that may occur in the goroutine
				//if r := recover(); r != nil {
				//	fmt.Printf("%s Recovered panic in goroutine %s: %v\n", serverName, name, r)
				//}

				// If tracking is enabled, decrement the number of routines and remove from the map
				atomic.AddInt64(&g.numRoutines, -1)
				g.routineMap.Delete(id)

			}()

			// Run the provided function for the goroutine
			f()
		}()
	} else {
		go func() {
			f()
		}()
	}

}

func (g *grTracking) logActiveGoRoutines() {
	fmt.Printf("Go routines left in tracker: %v\n", atomic.LoadInt64(&g.numRoutines))
	fmt.Println("Go routines in tracker:")

	g.routineMap.Range(func(key, value interface{}) bool {
		fmt.Printf("Goroutine -- %v %s\n", key, value)
		return true // continue iteration
	})
}

// TODO Need to check we can't get a cast error here
func percMakeup(known, want int) uint8 {
	if want == 0 {
		return 0 // Prevent division by zero
	}
	return uint8((known * 100) / want)
}

func printStartup(s *GBServer) {

	fmt.Printf(`
========================================================================
	   ______      ____          ____             __           
	  / ____/___  / __/__  _____/ __ )_________  / /_____      
	 / / __/ __ \/ /_/ _ \/ ___/ __  / ___/ __ \/ //_/ _ \     
	/ /_/ / /_/ / __/  __/ /  / /_/ / /  / /_/ / ,< /  __/     
	\____/\____/_/  \___/_/  /_____/_/   \____/_/|_|\___/      
	
========================================================================
	`)
	fmt.Println()
	fmt.Printf("  Go Version         : %s\n", runtime.Version())
	fmt.Printf("  GoferBroke Version : %s\n", version.Version)
	fmt.Printf("  GoferBroke Commit  : %s\n", version.Commit)
	fmt.Printf("  Server Created on  : %s\n", time.Now().Format(time.DateTime))
	fmt.Printf("  Server UUID        : %s\n", s.ServerName)
	fmt.Printf("  Name of node       : %s\n", s.name)
	fmt.Printf("  Part of cluser     : %s\n", s.gbClusterConfig.Name)
	fmt.Printf("  Configured addr    : %s\n", s.boundTCPAddr)
	fmt.Printf("  Host               : %s\n", s.sm.HostName)
	fmt.Printf("  Host ID            : %s\n", s.sm.HostID)
	fmt.Printf("  Platform           : %s\n", s.sm.Platform)
	fmt.Printf("  Platform Family    : %s\n", s.sm.PlatformFamily)
	fmt.Printf("  Total Memory       : %d\n", s.sm.TotalMemory)
	fmt.Printf("  CPU                : %s\n", s.sm.ModelName)
	fmt.Printf("  Cores              : %d\n", s.sm.Cores)
	fmt.Println()

}

//==========================================================================================

// For tests

func int64ToBytes(n int64) []byte {
	buf := make([]byte, 8) // Allocate 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

var heartBeatKV = map[string]*Delta{
	MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_): {KeyGroup: SYSTEM_DKG, Key: _HEARTBEAT_, ValueType: D_INT64_TYPE, Version: 1640995204, Value: int64ToBytes(1640995204)},
}

var keyValues1 = map[string]*Delta{
	"TEST:key6":  {KeyGroup: "TEST", Key: "key6", ValueType: D_STRING_TYPE, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
	"TEST:key7":  {KeyGroup: "TEST", Key: "key7", ValueType: D_STRING_TYPE, Version: 1640995205, Value: []byte("A")},
	"TEST:key8":  {KeyGroup: "TEST", Key: "key8", ValueType: D_STRING_TYPE, Version: 1640995206, Value: []byte("Test serialization with repeated values. Test serialization with repeated values.")},
	"TEST:key9":  {KeyGroup: "TEST", Key: "key9", ValueType: D_STRING_TYPE, Version: 1640995207, Value: []byte("😃 Emoji support test.")},
	"TEST:key10": {KeyGroup: "TEST", Key: "key10", ValueType: D_STRING_TYPE, Version: 1640995208, Value: []byte("Another simple string.")},
}

var keyValues1LowerVersion = map[string]*Delta{
	"TEST:key6":  {KeyGroup: "TEST", Key: "key6", ValueType: D_STRING_TYPE, Version: 1640995204, Value: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")},
	"TEST:key7":  {KeyGroup: "TEST", Key: "key7", ValueType: D_STRING_TYPE, Version: 1640995205, Value: []byte("A")},
	"TEST:key8":  {KeyGroup: "TEST", Key: "key8", ValueType: D_STRING_TYPE, Version: 1640995201, Value: []byte("Test serialization with repeated values.")},
	"TEST:key9":  {KeyGroup: "TEST", Key: "key9", ValueType: D_STRING_TYPE, Version: 1640995202, Value: []byte("😃")},
	"TEST:key10": {KeyGroup: "TEST", Key: "key10", ValueType: D_STRING_TYPE, Version: 1640995207, Value: []byte("Another string")},
}

var addressTestingKVs = map[string]*Delta{
	"address:tcp": {KeyGroup: ADDR_DKG, Key: _ADDRESS_, ValueType: D_STRING_TYPE, Version: 1640995204, Value: []byte("127.0.0.1")},
}

var multipleAddressTestingKVs = map[string]*Delta{
	"address:tcp":   {KeyGroup: ADDR_DKG, Key: _ADDRESS_, ValueType: D_STRING_TYPE, Version: 1640995204, Value: []byte("127.0.0.1")},
	"address:CLOUD": {KeyGroup: ADDR_DKG, Key: "CLOUD", ValueType: D_STRING_TYPE, Version: 1640995204, Value: []byte("137.184.248.0")},
	"address:DNS":   {KeyGroup: ADDR_DKG, Key: "DNS", ValueType: D_STRING_TYPE, Version: 1640995204, Value: []byte("example.com")},
}

var keyValues2 = map[string]*Delta{
	"address:tcp":        {KeyGroup: ADDR_DKG, Key: _ADDRESS_, ValueType: D_STRING_TYPE, Version: 1640995204, Value: []byte("127.0.0.1")},
	"address:NODE_CONNS": {KeyGroup: ADDR_DKG, Key: _NODE_CONNS_, ValueType: D_INT_TYPE, Version: 1640995205, Value: []byte{0}},
	"address:HEARTBEAT":  {KeyGroup: ADDR_DKG, Key: _HEARTBEAT_, ValueType: D_INT64_TYPE, Version: 1640995206, Value: int64ToBytes(1640995206)},
}

// TODO Make another one of these but with config

func GenerateDefaultTestServer(serverName string, kv map[string]*Delta, numParticipants int) *GBServer {

	if numParticipants == 0 {
		numParticipants = 1
	}

	nodeCfg := InitDefaultNodeConfig()
	logger, fl, jrb := setupLogger(context.Background(), nodeCfg)

	// Mock server setup
	gbs := &GBServer{
		clusterMap: ClusterMap{
			participants:     make(map[string]*Participant, numParticipants),
			participantArray: make([]string, numParticipants),
		},
		ServerName:      serverName,
		gbClusterConfig: InitDefaultClusterConfig(),
		gbNodeConfig:    nodeCfg,
		logger:          logger,
		slogHandler:     fl,
		jsonBuffer:      jrb,
		event:           NewEventDispatcher(),
	}

	maxV := int64(0)

	for _, value := range kv {
		if value.Version > maxV {
			maxV = value.Version
		}
	}

	gbs.numNodeConnections = int64(numParticipants)

	mainPart := &Participant{
		name:       serverName,
		keyValues:  kv,
		maxVersion: maxV,
	}

	gbs.clusterMap.participantArray[0] = mainPart.name

	gbs.clusterMap.participants[gbs.ServerName] = mainPart

	if numParticipants == 1 {
		return gbs
	}

	for i := 1; i < numParticipants; i++ {

		participantName := fmt.Sprintf("node-test-%d", i)
		gbs.name = participantName

		// Create a participant
		participant := &Participant{
			name:       participantName,
			keyValues:  kv,
			maxVersion: maxV,
		}

		gbs.clusterMap.participantArray[i] = gbs.name

		// Add participant to the ClusterMap
		gbs.clusterMap.participants[participantName] = participant

	}

	return gbs

}
