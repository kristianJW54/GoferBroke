package src

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"testing"
	"time"
)

//TODO need test to help with node reconnect to seed

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
	}

	gbs := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2 := NewServer("test-server-2", 2, config, "localhost", "8082", "8083", lc)
	go gbs.StartServer()
	time.Sleep(1 * time.Second)
	go gbs2.StartServer()

	// Current break is here

	//client := gbs.tmpClientStore["1"]
	//client2 := gbs2.tmpClientStore["1"]

	//log.Printf("%s --> temp client is %s --> direction %s", gbs.ServerName, client.Name, client.directionType)
	//log.Printf("%s --> temp client is %s --> direction %s", gbs2.ServerName, client2.Name, client2.directionType)

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

// Simulate Network Loss by Closing Existing Connections
func (s *GBServer) simulateConnectionLoss() {
	s.serverLock.Lock() // Lock to prevent changes to the connection map during operation
	defer s.serverLock.Unlock()

	// Close all active client connections
	for _, client := range s.tmpClientStore {
		err := client.gbc.Close() // Assuming client.conn is of type net.Conn
		if err != nil {
			log.Printf("Failed to close client connection: %v", err)
		}
	}

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

	gbs := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2 := NewServer("test-server-2", 1, config, "localhost", "8082", "8083", lc)
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

	//log.Printf("%s --> temp client is %s --> direction %s", gbs.ServerName, client.Name, client.directionType)
	//log.Printf("%s --> temp client is %s --> direction %s", gbs2.ServerName, client2.Name, client2.directionType)

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

	gbs := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)

	go gbs.StartServer()

	time.Sleep(1 * time.Second)
	t.Logf("self name: %s", gbs.selfInfo.name)
	for k, value := range gbs.selfInfo.keyValues {
		t.Logf("key: %s value: %d", k, value)
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

	gbs := NewServer("test-server", 1, config, "localhost", "8081", "8080", lc)
	gbs2 := NewServer("test-server 2", 1, config, "localhost", "8082", "8083", lc)
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
