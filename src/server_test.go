package src

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestServerNameLengthError(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   ip,
				SeedPort: port,
			},
		},
	}

	_, err := NewServer("test-server-name-long-error", 1, config, "localhost", "8081", "8080", lc)
	if err == nil {
		t.Errorf("TestServerNameLengthError should have returned an error")
	}

	log.Printf("error = %v", err)

}

func TestServerRunningTwoNodes(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   ip,
				SeedPort: port,
			},
		},
		Internal: &InternalOptions{},
		Cluster:  &ClusterOptions{},
	}

	gbs, _ := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server-2", 2, config, "localhost", "8082", "8083", lc)

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()

	// Current break is here

	//client := gbs.tmpClientStore["1"]
	//client2 := gbs2.tmpClientStore["1"]

	//log.Printf("%s --> temp client is %s --> direction %s", gbs.ServerName, client.name, client.directionType)
	//log.Printf("%s --> temp client is %s --> direction %s", gbs2.ServerName, client2.name, client2.directionType)

	time.Sleep(3 * time.Second)
	gbs2.Shutdown()
	//time.Sleep(1 * time.Second)
	gbs.Shutdown()
	time.Sleep(1 * time.Second)

	for k, v := range gbs.clusterMap.participants {
		log.Printf("name = %s", k)
		for _, value := range v.keyValues {
			log.Printf("value = %+s", value.value)
		}
	}

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()

}

func TestGossipSignal(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   ip,
				SeedPort: port,
			},
		},
		Internal: &InternalOptions{},
		Cluster:  &ClusterOptions{},
	}

	gbs, _ := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server", 2, config, "localhost", "8082", "8083", lc)
	gbs3, _ := NewServer("test-server", 3, config, "localhost", "8085", "8084", lc)

	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()
	time.Sleep(1 * time.Second)
	go gbs3.StartServer()
	time.Sleep(6 * time.Second)

	gbs2.serverContext.Done()
	gbs3.serverContext.Done()
	gbs.serverContext.Done()

	go gbs2.Shutdown()
	go gbs3.Shutdown()
	go gbs.Shutdown()

	time.Sleep(3 * time.Second)

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
	gbs3.logActiveGoRoutines()

}

//======================================================
// Testing gossip with different states for nodes
//======================================================

func TestGossipDifferentStates(t *testing.T) {

	// TODO make some different cluster map states for nodes about other nodes

	// Mock timestamps to use
	// 1735988400 = Sat Jan 04 2025 11:00:00 GMT+0000
	// 1735988535 = Sat Jan 04 2025 11:02:15 GMT+0000 --> + 2min 15sec
	// 1735988555 = Sat Jan 04 2025 11:02:35 GMT+0000 --> + 2min 35sec
	// 1735988625 = Sat Jan 04 2025 11:03:45 GMT+0000 --> + 3min 45sec
	// 1735988700 = Sat Jan 04 2025 11:05:00 GMT+0000 --> + 5min

	// TODO will need to change the participant timestamp and names to a standard

	lc := net.ListenConfig{}

	testConfig := &InternalOptions{
		isTestMode:                            true,
		disableGossip:                         false,
		disableInitialiseSelf:                 false,
		disableInternalGossipSystemUpdate:     true,
		disableUpdateServerTimeStampOnStartup: true,
	}
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   "127.0.0.1",
				SeedPort: "8081",
			},
		},
		Internal: testConfig,
	}

	gbs, _ := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server", 2, config, "localhost", "8082", "8080", lc)

	UpdateServerNameAndTime(t, gbs, 1735988400)
	UpdateServerNameAndTime(t, gbs2, 1735988401)

	// TODO Give each node a different cluster map to converge on and gossip about

	go gbs.StartServer()
	go gbs2.StartServer()

	time.Sleep(5 * time.Second)

	go gbs.Shutdown()
	go gbs2.Shutdown()

	time.Sleep(1 * time.Second)

}

func UpdateServerNameAndTime(t testing.TB, node *GBServer, time uint64) {
	t.Helper()

	node.ServerID.timeUnix = time
	node.ServerName = node.String()

}

//======================================================

// Simulate Network Loss by Closing Existing Connections
func (s *GBServer) simulateConnectionLoss() {
	s.serverLock.Lock() // Lock to prevent changes to the connection map during operation
	defer s.serverLock.Unlock()

	// Close all active client connections
	s.tmpConnStore.Range(func(key, value interface{}) bool {
		log.Printf("closing temp node %s", key)
		c, ok := value.(*gbClient)
		if !ok {
			log.Printf("Error: expected *gbClient but got %T for key %v", value, key)
			return true // Continue iteration
		}
		if c.gbc != nil {
			c.gbc.Close()
		}
		s.tmpConnStore.Delete(key)
		return true
	})

	log.Println("All existing connections have been closed to simulate network loss")
}

// TODO After node 2 reconnects - node 1 duplicates serialisation and adding to cluster causing move to connected failure

func TestReconnectOfNode(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   ip,
				SeedPort: port,
			},
		},
	}

	gbs, _ := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server", 2, config, "localhost", "8082", "8083", lc)
	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()
	time.Sleep(2 * time.Second)
	//gbs.simulateConnectionLoss()
	gbs2.Shutdown()
	// Current break is here
	time.Sleep(2 * time.Second)
	go gbs2.StartServer()
	//client := gbs.tmpClientStore["1"]
	//client2 := gbs2.tmpClientStore["1"]

	//log.Printf("%s --> temp client is %s --> direction %s", gbs.ServerName, client.name, client.directionType)
	//log.Printf("%s --> temp client is %s --> direction %s", gbs2.ServerName, client2.name, client2.directionType)

	time.Sleep(2 * time.Second)
	gbs2.Shutdown()
	time.Sleep(1 * time.Second)
	gbs.Shutdown()
	time.Sleep(1 * time.Second)

	for k, v := range gbs.clusterMap.participants {
		log.Printf("name = %s", k)
		for _, value := range v.keyValues {
			log.Printf("value = %+s", value.value)
		}
	}

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()

}

func TestServerRunningOneNodes(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   ip,
				SeedPort: port,
			},
		},
	}

	log.Println(config)

	gbs, _ := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)

	go gbs.StartServer()

	selfInfo := gbs.getSelfInfo()

	time.Sleep(1 * time.Second)
	t.Logf("self name: %s", selfInfo.name)
	for k, value := range selfInfo.keyValues {
		t.Logf("key: %s value: %v", k, value)
	}

	time.Sleep(3 * time.Second)

	go gbs.Shutdown()
	time.Sleep(2 * time.Second)

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fmt.Printf("Total allocated memory: %d bytes\n", mem.TotalAlloc)
	fmt.Printf("Number of memory allocations: %d\n", mem.Mallocs)

	gbs.logActiveGoRoutines()

	time.Sleep(1 * time.Second)

}

func TestInfoSend(t *testing.T) {

	lc := net.ListenConfig{}

	ip := "127.0.0.1" // Use the full IP address
	port := "8081"

	// Initialize config with the seed server address
	config := &GbConfig{
		SeedServers: []Seeds{
			{
				SeedIP:   ip,
				SeedPort: port,
			},
		},
	}

	//log.Println(config)

	gbs, _ := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2, _ := NewServer("test-server 2", 1, config, "localhost", "8082", "8083", lc)
	go gbs.StartServer()
	//time.Sleep(1 * time.Second)
	go gbs2.StartServer()
	time.Sleep(3 * time.Second)

	go gbs.Shutdown()
	go gbs2.Shutdown()
	time.Sleep(1 * time.Second)
	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()
}
