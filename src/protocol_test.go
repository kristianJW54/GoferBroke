package src

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestDataTransfer(t *testing.T) {
	lc := net.ListenConfig{}

	ip := net.ParseIP("localhost:8081")
	port := 8081

	confAddr := &net.TCPAddr{IP: ip, Port: port}

	config := &Config{
		confAddr,
	}

	gbs := NewServer("test-server", config, "localhost", "8081", lc)

	go gbs.StartServer()

	time.Sleep(1 * time.Second)
	// Dial the TCP server
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	fmt.Println("Connected to the server.")

	time.Sleep(1 * time.Second)

	// Create and send payload
	payload := mockDataConn(t)

	_, err = conn.Write(payload)
	if err != nil {
		t.Fatal(err)
	}

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

	// Allow time for the server to start listening
	time.Sleep(100 * time.Millisecond)

}

func mockDataConn(t *testing.T) []byte {
	t.Helper()

	data := []byte("I am a test, I always have been a test. :(")
	length := uint16(len(data))

	// Set up the header
	header := ProtoHeader{
		ProtoVersion:  PROTO_VERSION_1,
		ClientType:    NODE,
		MessageType:   TEST,
		Command:       GOSSIP,
		MessageLength: length,
	}

	// Create the GossipPayload
	payload := &TCPPayload{
		Header: header,
		Data:   data,
	}

	// Marshal the payload into a byte slice to simulate sending it over the network
	b, err := payload.MarshallBinary()
	if err != nil {
		t.Fatal("Error marshalling mock data:", err)
	}

	return b // Return the marshalled byte slice
}
