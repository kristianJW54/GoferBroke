package src

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"testing"
	"time"
)

// Delta Header should be --> Command[V], MessageLength[0 0], KeyLength[0], ValueLength [0 0]

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
	key := "newAccount.log"
	value := "new-accounts:[100500]\r\n"

	deltaMessage := fmt.Sprintf("V: %s %s", key, value)

	hdr := make([]byte, 6+2)
	offset := 0
	hdr[0] = deltaMessage[0]
	offset += 1
	binary.BigEndian.PutUint16(hdr[offset:], uint16(len(deltaMessage)))
	log.Printf("length of deltaMessage: %v\n", uint16(len(deltaMessage)))
	offset += 2
	// Adding in key length
	hdr[offset] = uint8(len(key))
	log.Printf("key length: %d\n", len(key))
	offset += 1
	// Adding in value length
	binary.BigEndian.PutUint16(hdr[offset:], uint16(len(value)))
	offset += 2
	copy(hdr[len(hdr)-2:], "\r\n") // Adding CLRF at the end
	offset += 2
	log.Printf("header 1 = %v", hdr)
	formattedMessage := append(hdr, []byte(deltaMessage)...)

	// Send the message over the connection
	_, err = conn.Write(formattedMessage) // Ensure to add the newline for your parser to detect the end
	if err != nil {
		fmt.Printf("Failed to send message: %v\n", err)
		return
	}

	key2 := "accountChange.user123"
	value2 := "[{\"username\":\"user123\",\"password\":\"imapassword\",\"pin\":\"0000\"}}]\r\n"

	deltaMessage2 := fmt.Sprintf("V: %s %s", key2, value2)

	hdr2 := make([]byte, 6+2)
	offset2 := 0
	hdr2[0] = deltaMessage2[0]
	offset2 += 1
	binary.BigEndian.PutUint16(hdr2[offset2:], uint16(len(deltaMessage2)))
	offset2 += 2
	// Adding key length
	hdr2[offset2] = uint8(len(key2))
	offset2 += 1
	binary.BigEndian.PutUint16(hdr2[offset2:], uint16(len(value2)))
	offset2 += 2
	copy(hdr2[len(hdr2)-2:], "\r\n") // Adding CLRF at the end
	offset2 += 2
	log.Printf("header 1 = %v", hdr2)
	formattedMessage2 := append(hdr2, []byte(deltaMessage2)...)

	//time.Sleep(1 * time.Second)
	// Send the message over the connection
	_, err = conn.Write(formattedMessage2) // Ensure to add the newline for your parser to detect the end
	if err != nil {
		fmt.Printf("Failed to send message: %v\n", err)
		return
	}

	srvDelta := gbs.selfInfo
	for _, value := range srvDelta.valueIndex {
		log.Printf("%s --> key = %s value = %s", srvDelta.name, value, srvDelta.keyValues[value].value)
	}

	time.Sleep(1 * time.Second)

	//Check the cluster map is the same
	clusterD := gbs.clusterMap
	for _, value := range clusterD.participants[gbs.ServerName].valueIndex {
		log.Printf("%s --> key = %s value = %s", gbs.ServerName, value, clusterD.participants[gbs.ServerName].keyValues[value].value)
	}

	go gbs.Shutdown()
	time.Sleep(1 * time.Second)
	gbs.logActiveGoRoutines()

}
