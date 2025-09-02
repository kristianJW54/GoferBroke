package cluster

import (
	"bytes"
	"encoding/binary"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"log"
	"strings"
	"testing"
	"time"
)

// Test Ready ✅
func TestServerNameLengthError(t *testing.T) {

	nodeCfg := `
				Name = "iamaverylongnamewhichshouldneverbeallowedtobeconfiguredbecauseigetauuidanywaysonowimjustbeinggreedy"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True	
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	_, err := NewServerFromConfigString(nodeCfg, cfg)
	if err == nil {
		t.Errorf("expected error of exceed name length")
		return
	}
}

// Test Ready ✅
func TestNewServerFunction(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	if gbs.addr != "localhost:8081" {
		t.Errorf("expected bound TCP address to be localhost:8081")
	}

	if !gbs.isSeed {
		t.Errorf("expected isSeed to be true")
	}

	if gbs.gbClusterConfig.Cluster.ClusterNetworkType != C_LOCAL {
		t.Errorf("expected ClusterNetworkType to be 4 got %d", gbs.gbClusterConfig.Cluster.ClusterNetworkType)
	}

}

// Test Ready ✅
func TestWrongServerMode(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"

				// We should error here because we are saying we are not a seed but we have the same seed addr in cluster config
				IsSeed = False

				Internal { 
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	_, err := NewServerFromConfigString(nodeCfg, cfg)
	if err == nil {
		t.Errorf("expected an error")
	} else {
		if !strings.Contains(err.Error(), "server is NOT configured as a seed, but matches a listed seed address — change config to mark it as a seed OR use a different address") {
			t.Errorf("expected error to contain 'invalid server mode', got: %v", err)
		}
	}

}

// Test Ready ✅
func TestServerStartup(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
					LogToBuffer = False
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	gbs.StartServer()

	time.Sleep(100 * time.Millisecond)

	if gbs.nodeListener == nil {
		t.Errorf("node listener was not created")
	}
	if gbs.ServerName == "" {
		t.Errorf("server name was not created")
	}
	if gbs.flags.isSet(SHUTTING_DOWN) {
		t.Errorf("expected server to not be shutting down during start")
	}

	gbs.Shutdown()

}

// Test Ready ✅
func TestInitParticipant(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	p, _ := gbs.initSelfParticipant()

	if _, exists := p.keyValues[MakeDeltaKey(ADDR_DKG, _ADDRESS_)]; !exists {
		t.Errorf("expected to find address key initialised")
	}
	if name, exists := p.keyValues[MakeDeltaKey(CONFIG_DKG, "Name")]; !exists {
		t.Errorf("expected to find cluster config key initialised")
	} else if string(name.Value) != "default-local-cluster" {
		t.Errorf("expected cluster config key to be default-local-cluster")
	}

}

// Test Ready ✅
func TestIncrementNodeConnCount(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	selfInfo, err := gbs.initSelfParticipant()
	if err != nil {
		return
	}
	gbs.clusterMap = *initClusterMap(gbs.ServerName, gbs.boundTCPAddr, selfInfo)

	gbs.incrementNodeConnCount()

	connCount := gbs.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(SYSTEM_DKG, _NODE_CONNS_)].Value

	if binary.BigEndian.Uint16(connCount) != uint16(1) {
		t.Errorf("expected connection count to be 1, got %d", connCount)
	}

	gbs.incrementNodeConnCount()
	gbs.incrementNodeConnCount()
	gbs.incrementNodeConnCount()

	connCount = gbs.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(SYSTEM_DKG, _NODE_CONNS_)].Value
	if binary.BigEndian.Uint16(connCount) != 4 {
		t.Errorf("expected connection count to be 4, got %d", binary.BigEndian.Uint16(connCount))
	}

}

// Test Ready ✅
func TestDecreaseNodeConnCount(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	selfInfo, err := gbs.initSelfParticipant()
	if err != nil {
		return
	}
	gbs.clusterMap = *initClusterMap(gbs.ServerName, gbs.boundTCPAddr, selfInfo)

	gbs.incrementNodeConnCount()
	gbs.incrementNodeConnCount()
	gbs.incrementNodeConnCount()
	gbs.incrementNodeConnCount()

	gbs.decrementNodeConnCount()

	count := gbs.clusterMap.participants[gbs.ServerName].keyValues[MakeDeltaKey(SYSTEM_DKG, _NODE_CONNS_)].Value
	if binary.BigEndian.Uint16(count) != uint16(3) {
		t.Errorf("expected connection count to be 3, got %d", count)
	}
	if gbs.numNodeConnections != 3 {
		t.Errorf("expected number of connections to be 3, got %d", gbs.numNodeConnections)
	}

}

// Test Ready ✅
func TestUpdateHeartBeat(t *testing.T) {

	keyValues := map[string]*Delta{
		"system:heartbeat": &Delta{KeyGroup: SYSTEM_DKG, ValueType: D_BYTE_TYPE, Key: _HEARTBEAT_, Version: 1640995200, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		"system:tcp":       &Delta{KeyGroup: SYSTEM_DKG, ValueType: D_BYTE_TYPE, Key: _ADDRESS_, Version: 1640995200, Value: []byte("127.0.0.0.1:8081")},
	}

	// Initialize config with the seed server address
	gbs := GenerateDefaultTestServer("test-1", keyValues, 0)

	self := gbs.GetSelfInfo()

	oldHeartBeat := self.keyValues[MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_)].Version

	err := gbs.updateHeartBeat(time.Now().Unix())
	if err != nil {
		log.Fatal(err)
	}

	newHeartBeat := self.keyValues[MakeDeltaKey(SYSTEM_DKG, _HEARTBEAT_)].Version

	if newHeartBeat == oldHeartBeat {
		t.Errorf("new heart beat version not updated")
	}

}

//==============================
// Live Unit Tests
//==============================

// Test Ready ✅
func TestServerRunningTwoNodes(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	nodeCfg2 := `
				Name = "t-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	gbs2, err := NewServerFromConfigString(nodeCfg2, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	time.Sleep(100 * time.Millisecond)
	go gbs2.StartServer()

	time.Sleep(5 * time.Second)

	gbs2.Shutdown()
	gbs.Shutdown()

	time.Sleep(100 * time.Millisecond)

	// Test

	if node2, exists := gbs.clusterMap.participants[gbs2.ServerName]; !exists {
		t.Errorf("expected to have %s in %s's ClusterMap", gbs2.name, gbs.name)
	} else {
		addr := node2.keyValues[MakeDeltaKey(ADDR_DKG, _ADDRESS_)].Value
		if string(addr) != "localhost:8082" {
			t.Errorf("expected address to be localhost:8082, got %s", string(addr))
		}
	}

}

// Test Ready ✅
func TestServerShutDownConfigFail(t *testing.T) {

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True	
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	node2Cfg := `
				Name = "node-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	cfg2 := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 2
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}
	gbs2, err := NewServerFromConfigString(node2Cfg, cfg2)
	if err != nil {
		t.Errorf("%v", err)
	}

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()

	select {
	case err := <-gbs2.fatalErrorCh:
		if err != nil {
			he := Errors.HandleError(err, func(gbErrors []*Errors.GBError) error {

				for _, gbError := range gbErrors {

					if gbError.Code == Errors.CONFIG_CHECKSUM_FAIL_CODE {
						return gbError
					}
				}
				return nil

			})

			if he == nil {
				t.Errorf("expected to have found fatal error %d - got nil instead", Errors.CONFIG_CHECKSUM_FAIL_CODE)
			}

		} else {
			t.Errorf("fatalErrorCh received nil error")
		}
	case <-time.After(5 * time.Second):
		t.Error("Timed out waiting for fatal error")
	}

	gbs.Shutdown()

}

// Test Ready ✅
func TestServerUpdateSelfConfig(t *testing.T) {

	nodeCfg := `
				Name = "node-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True	
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("%v", err)
	}

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	gbs.Shutdown()

	clusterKey := "Name"
	newValue := "new-cluster-name"

	deltaUpdate := &Delta{
		KeyGroup:  CONFIG_DKG,
		Key:       clusterKey,
		Version:   1,
		ValueType: D_STRING_TYPE,
		Value:     []byte(newValue),
	}

	// Now we change a config in the cluster

	if _, exists := gbs.clusterMap.participants[gbs.ServerName].keyValues["config:Name"]; exists {
		err := gbs.UpdateClusterConfigDeltaAndSelf(deltaUpdate.Key, deltaUpdate)
		if err != nil {
			t.Errorf("%v", err)
		}
		d := gbs.clusterMap.participants[gbs.ServerName].keyValues["config:Name"]

		if bytes.Compare(d.Value, deltaUpdate.Value) != 0 {
			t.Errorf("wanted %s got %s", newValue, d.Value)
		}

	}

}

// Test Ready ✅
func TestServerNewDeltaEvent(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	nodeCfg2 := `
				Name = "t-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	gbs2, err := NewServerFromConfigString(nodeCfg2, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	gbs.AddHandlerRegistration(func(s *GBServer) error {
		_, err := s.AddHandler(s.ServerContext, NewDeltaAdded, false, func(event Event) error {

			if delta, ok := event.Payload.(*DeltaAddedEvent); !ok {
				t.Errorf("expected delta event of type DeltaAdded")
			} else {

				if delta.DeltaKey != "test:also-a-test" {
					t.Errorf("expected delta.Key to be also-a-test, got %s", delta.DeltaKey)
				}

			}
			return nil
		})
		return err
	})

	time.Sleep(100 * time.Millisecond)

	go gbs.StartServer()

	go gbs2.StartServer()

	time.Sleep(2 * time.Second)
	newDelta := &Delta{
		KeyGroup:  "test",
		Key:       "also-a-test",
		Version:   time.Now().Unix(),
		ValueType: D_BYTE_TYPE,
		Value:     []byte{},
	}
	self := gbs2.GetSelfInfo()
	err = self.Store(newDelta)
	if err != nil {
		t.Errorf("%v", err)
	}

	time.Sleep(3 * time.Second)

	gbs2.Shutdown()
	gbs.Shutdown()

	time.Sleep(100 * time.Millisecond)

	if d, exists := gbs.clusterMap.participants[gbs2.ServerName].keyValues[MakeDeltaKey("test", "also-a-test")]; !exists {
		t.Errorf("expected to have found delta - got %v", d)
	}

}

// Test Ready ✅
func TestBackgroundJobScheduler(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = True
					DisableStartupMessage = True
					DefaultLoggerEnabled = False
				}

`
	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	t.Run("single job", func(t *testing.T) {

		srv, err := NewServerFromConfigString(nodeCfg, cfg)
		if err != nil {
			t.Errorf("error creating new server: %v", err)
		}

		srv.bj.registerBackgroundJob(
			2*time.Second,
			1*time.Second,
			func() {
				srv.numNodeConnections += 1
			},
		)
		go srv.StartServer()

		time.Sleep(7 * time.Second)

		srv.Shutdown()

		if srv.numNodeConnections < 2 {
			t.Errorf("expected to have at least 2 connections, got %d", srv.numNodeConnections)
		}

	})

	t.Run("multi job", func(t *testing.T) {

		srv, err := NewServerFromConfigString(nodeCfg, cfg)
		if err != nil {
			t.Errorf("error creating new server: %v", err)
		}

		srv.bj.registerBackgroundJob(
			3*time.Second,
			1*time.Second,
			func() {
				srv.numNodeConnections += 1
			},
		)

		srv.bj.registerBackgroundJob(
			2*time.Second,
			1*time.Second,
			func() {
				srv.numClientConnections += 1
			},
		)

		go srv.StartServer()

		time.Sleep(10 * time.Second)

		srv.Shutdown()

		// For a duration of - client conns should be at least 4 if run every 2 seconds - node should be at least 3

		if srv.numNodeConnections < 3 {
			t.Errorf("expected to have at least 2 connections, got %d", srv.numNodeConnections)
		}
		if srv.numClientConnections < 4 {
			t.Errorf("expected to have at least 4 connections, got %d", srv.numClientConnections)
		}

	})

}

func TestDeadNodeHandling(t *testing.T) {

	nodeCfg := `
				Name = "t-1"
				Host = "localhost"
				Port = "8081"
				IsSeed = True
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = True
				}

`

	nodeCfg2 := `
				Name = "t-2"
				Host = "localhost"
				Port = "8082"
				IsSeed = False
				Internal {
					DisableGossip = False
					DisableStartupMessage = True
					DefaultLoggerEnabled = True
				}

`

	cfg := `Name = "default-local-cluster"
	SeedServers = [
	   {Host: "localhost", Port: "8081"},
	]
	Cluster {
	   ClusterNetworkType = "LOCAL"
	   NodeSelectionPerGossipRound = 1
	}`

	gbs, err := NewServerFromConfigString(nodeCfg, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	gbs2, err := NewServerFromConfigString(nodeCfg2, cfg)
	if err != nil {
		t.Errorf("error creating new server: %v", err)
	}

	go gbs.StartServer()
	go gbs2.StartServer()

	time.Sleep(20 * time.Second)

	gbs2.Shutdown()

	time.Sleep(10 * time.Second)

	gbs.Shutdown()

}
