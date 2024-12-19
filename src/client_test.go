package src

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"testing"
	"time"
)

func TestClientDelta(t *testing.T) {

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

	// Dial the TCP server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}

	// Format the message for a CACHE_UPDATE delta
	key := "CACHE_UPDATE"
	value := "user123:password\r\n"
	timestamp := "1697785200"

	deltaMessage := fmt.Sprintf("V: %s %s %s", key, timestamp, value)

	hdr := make([]byte, 4+1)
	hdr[0] = deltaMessage[0]
	binary.BigEndian.PutUint16(hdr[1:3], uint16(len(deltaMessage)))
	copy(hdr[3:], deltaMessage[3:]) // Copy the deltaMessage starting from position 3
	copy(hdr[len(hdr)-2:], "\r\n")  // Adding CLRF at the end

	formattedMessage := append(hdr, []byte(deltaMessage)...)

	// Send the message over the connection
	_, err = conn.Write(formattedMessage) // Ensure to add the newline for your parser to detect the end
	if err != nil {
		fmt.Printf("Failed to send message: %v\n", err)
		return
	}

	log.Printf("Sent message: %s", formattedMessage)

	log.Printf("Connected to server %s", conn.RemoteAddr())

	time.Sleep(5 * time.Second)

}
