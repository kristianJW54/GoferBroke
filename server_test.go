package main

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

	gbs := NewServer("test-server", "localhost", "8081", lc)

	go gbs.StartServer()

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
