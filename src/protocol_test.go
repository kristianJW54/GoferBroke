package src

import (
	"fmt"
	"net"
	"testing"
	"time"
)

//func TestHeaderCreation(t *testing.T) {
//
//	header := newProtoHeader(1, 0, 4, 1)
//	_, err := header.headerSerialize()
//	if err != nil {
//		t.Error(err)
//	}
//
//	t.Log("header: ", *header)
//
//}

func TestAcceptConnection(t *testing.T) {
	lc := net.ListenConfig{}

	ip := net.ParseIP("127.0.0.1") // Use the full IP address
	port := 8081
	confAddr := &net.TCPAddr{IP: ip, Port: port}

	// Initialize config with the seed server address
	config := &Config{
		Seed: confAddr, // Ensure the seed contains both the IP and port
	}

	gbs := NewServer("test-server", config, "localhost", "8081", "8080", lc)

	go gbs.StartServer()

	time.Sleep(1 * time.Second)
	// Dial the TCP server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	fmt.Printf("Connected to the server %v\n", conn.RemoteAddr())

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
	length := uint32(len(data))

	// Set up the header
	header := &PacketHeader{
		ProtoVersion:  PROTO_VERSION_1,
		Command:       GOSS_ACK,
		MsgLength:     length,
		PayloadLength: 0,
	}

	// Create the GossipPayload
	payload := &TCPPacket{
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
