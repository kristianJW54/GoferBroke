package src

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"testing"
	"time"
)

//TODO refactor test to update the status only while the server is accepting connections
// if the server connects but closes then the status needs to be closed

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

	//log.Println(config)

	gbs := NewServer("test-server", config, "localhost", "8081", "8080", lc)
	gbs2 := NewServer("test-server-2", config, "localhost", "8082", "8083", lc)

	go gbs.StartServer()
	go gbs2.StartServer()

	time.Sleep(1 * time.Second)

	// Current break is here

	//client := gbs.tmpClientStore["1"]
	//client2 := gbs2.tmpClientStore["1"]

	//log.Printf("%s --> temp client is %s --> direction %s", gbs.ServerName, client.Name, client.directionType)
	//log.Printf("%s --> temp client is %s --> direction %s", gbs2.ServerName, client2.Name, client2.directionType)

	time.Sleep(4 * time.Second)

	go gbs.Shutdown()
	time.Sleep(2 * time.Second)
	go gbs2.Shutdown()

	time.Sleep(2 * time.Second)

	gbs.logActiveGoRoutines()
	gbs2.logActiveGoRoutines()

	time.Sleep(5 * time.Second)

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

	gbs := NewServer("test-server", config, "localhost", "8081", "8080", lc)

	go gbs.StartServer()

	time.Sleep(1 * time.Second)
	t.Logf("self name: %s", gbs.selfInfo.name)
	for k, value := range gbs.selfInfo.keyValues {
		t.Logf("key: %d value: %d", k, value)
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
