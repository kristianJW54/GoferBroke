package src

import (
	"fmt"
	"net"
	"testing"
	"time"
)

//TODO refactor test to update the status only while the server is accepting connections
// if the server connects but closes then the status needs to be closed

func TestServerRunning(t *testing.T) {

	lc := net.ListenConfig{}

	ip := net.ParseIP("127.0.0.1") // Use the full IP address
	port := 8081
	confAddr := &net.TCPAddr{IP: ip, Port: port}

	// Initialize config with the seed server address
	config := &Config{
		Seed: confAddr, // Ensure the seed contains both the IP and port
	}

	gbs := NewServer("test-server", config, "localhost", "8081", "8080", lc)
	gbs2 := NewServer("test-server-2", config, "localhost", "8082", "8080", lc)

	go gbs.StartServer()
	go gbs2.StartServer()

	time.Sleep(1 * time.Second)
	// Dial the TCP server
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	fmt.Println("Connected to the server.")

	//
	time.Sleep(5 * time.Second)
	//

	go gbs.Shutdown()
	go gbs2.Shutdown()

	// Wait for 10 seconds
	time.Sleep(10 * time.Second)

	// Close the connection after 10 seconds
	err = conn.Close()
	if err != nil {
		fmt.Printf("Error closing connection: %v\n", err)
		return
	}
	fmt.Println("Connection closed.")

	//fmt.Printf("Server Name: %s \nResult: %s\n", gbs.ServerName, gbs.Status())

}
