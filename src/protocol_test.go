package src

import (
	"encoding/binary"
	"fmt"
	"log"
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

	gbs := NewServer("test-server", config, "localhost", "8081", "8080", lc)

	go gbs.StartServer()

	time.Sleep(1 * time.Second)
	// Dial the TCP server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}

	fmt.Printf("Conn: Connected to the server remote --> %v local --> %v\n", conn.RemoteAddr(), conn.LocalAddr())

	time.Sleep(1 * time.Second)

	// Create and send payload
	//payload := mockDataConn(t)

	data := "I must not fear. Fear is the mind-killer. Fear is the little-death that brings total obliteration. " +
		"I will face my fear. I will permit it to pass over me and through me. And when it has gone past " +
		"I will turn the inner eye to see its path. Where the fear has gone there will be nothing. Only I will remain.\r\n"

	data2 := "I've known adventures, seen places you people will never see, I've been Offworld and back... frontiers! " +
		"I've stood on the back deck of a blinker bound for the Plutition Camps with sweat in my eyes watching stars " +
		"fight on the shoulder of Orion... I've felt wind in my hair, riding test boats off the black galaxies and " +
		"seen an attack fleet burn like a match and disappear. I've seen it, felt it...! I've seen things... seen things " +
		"you little people wouldn't believe. Attack ships on fire off the shoulder of Orion bright as magnesium... " +
		"I rode on the back decks of a blinker and watched C-beams glitter in the dark near the Tannhäuser Gate. " +
		"All those moments... they'll be gone\r\n"

	// Prepare first payload
	length := len(data)
	command := []byte{1}
	msgLength := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLength, uint32(length))

	payload := make([]byte, 1+4+2+length)
	payload[0] = command[0]
	copy(payload[1:5], msgLength)
	payload[5] = '\r'
	payload[6] = '\n'
	copy(payload[7:], data)

	// Send first payload
	_, err = conn.Write(payload)
	if err != nil {
		log.Fatal(err)
	}

	//// Prepare second payload
	length2 := len(data2)
	msgLength2 := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLength2, uint32(length2))

	payload2 := make([]byte, 1+4+2+length2)
	payload2[0] = command[0]
	copy(payload2[1:5], msgLength2)
	payload2[5] = '\r'
	payload2[6] = '\n'
	copy(payload2[7:], data2)

	// Send second payload
	time.Sleep(1 * time.Second)
	_, err = conn.Write(payload2)
	if err != nil {
		log.Fatal(err)
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
