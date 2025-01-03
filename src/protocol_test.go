package src

import (
	"fmt"
	"log"
	"net"
	"runtime"
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

	gbs := serverSetup()

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

	//packetWrite, packet2Write := sendTwoDataPackets()
	//
	//// Send first payload
	//_, err = conn.Write(packetWrite)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//_, err = conn.Write(packet2Write)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//var testData net.Buffers = [][]byte{
	//	[]byte{1, 1, 1, 0, 16, 0, 9, 13, 10, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 13, 10},
	//}
	//_, err = testData.WriteTo(conn)
	//if err != nil {
	//	log.Fatal(err)
	//}

	time.Sleep(5 * time.Second)

	go gbs.Shutdown()

	// Wait for 10 seconds
	time.Sleep(5 * time.Second)

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

func TestHighParseLoad(t *testing.T) {

	gbs := serverSetup()

	go gbs.StartServer()

	time.Sleep(1 * time.Second)

	// Dial the TCP server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}

	packetWrite, packet2Write := sendTwoDataPackets()

	time.Sleep(1 * time.Second)

	for i := 0; i < 2; i++ {

		t.Log("iteration -> ", i)

		_, err = conn.Write(packetWrite)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1 * time.Nanosecond) // to prevent overwhelming the read loop - remove once backpressure and flow control is in place
		_, err = conn.Write(packet2Write)
		if err != nil {
			log.Fatal(err)
		}

	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fmt.Printf("Total allocated memory: %d bytes\n", mem.TotalAlloc)
	fmt.Printf("Number of memory allocations: %d\n", mem.Mallocs)

}

func TestSeqRequestPool(t *testing.T) {

	gbs := serverSetup()

	go gbs.StartServer()

	seq1, err := gbs.acquireReqID()
	if err != nil {
		log.Fatal(err)
	}
	seq2, err := gbs.acquireReqID()
	if err != nil {
		log.Fatal(err)
	}

	t.Logf("sequence number 0 --> %d\n", seq1)
	t.Logf("sequence number 1 --> %d\n", seq2)

	time.Sleep(1 * time.Second)
	gbs.releaseReqID(seq2)
	time.Sleep(1 * time.Second)
	gbs.releaseReqID(seq1)
	time.Sleep(1 * time.Second)

	t.Log(seq1)
	t.Log(seq2)

	// Acquire again to verify reuse
	seq3, err := gbs.acquireReqID()
	if err != nil {
		log.Fatal(err)
	}
	t.Logf("Reacquired sequence number: %d", seq3)

	if seq3 != seq1 && seq3 != seq2 {
		t.Fatalf("Expected to reacquire released sequence number, got: %d", seq3)
	}

	time.Sleep(5 * time.Second)

	go gbs.Shutdown()

}

//================================================================
//----------------
// Helpers

func serverSetup() *GBServer {
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
	return gbs
}

func sendTwoDataPackets() ([]byte, []byte) {

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

	nh1 := constructNodeHeader(1, 2, uint8(2), uint16(len(data)), NODE_HEADER_SIZE_V1, 0, 0)
	packet := &nodePacket{
		nh1,
		[]byte(data),
	}
	packetWrite, err := packet.serialize()
	if err != nil {
		fmt.Printf("Failed to serialize packet: %v\n", err)
	}

	nh2 := constructNodeHeader(1, 2, uint8(2), uint16(len(data2)), NODE_HEADER_SIZE_V1, 0, 0)
	packet2 := &nodePacket{
		nh2,
		[]byte(data2),
	}
	packet2Write, err := packet2.serialize()
	if err != nil {
		fmt.Printf("Failed to serialize packet: %v\n", err)
	}

	return packetWrite, packet2Write

}

func sendOnePacket() []byte {

	data2 := "I've known adventures, seen places you people will never see, I've been Offworld and back... frontiers! " +
		"I've stood on the back deck of a blinker bound for the Plutition Camps with sweat in my eyes watching stars " +
		"fight on the shoulder of Orion... I've felt wind in my hair, riding test boats off the black galaxies and " +
		"seen an attack fleet burn like a match and disappear. I've seen it, felt it...! I've seen things... seen things " +
		"you little people wouldn't believe. Attack ships on fire off the shoulder of Orion bright as magnesium... " +
		"I rode on the back decks of a blinker and watched C-beams glitter in the dark near the Tannhäuser Gate. " +
		"All those moments... they'll be gone\r\n"

	nh2 := constructNodeHeader(1, 1, uint8(2), uint16(len(data2)), NODE_HEADER_SIZE_V1, 0, 0)
	packet2 := &nodePacket{
		nh2,
		[]byte(data2),
	}
	packet2Write, err := packet2.serialize()
	if err != nil {
		fmt.Printf("Failed to serialize packet: %v\n", err)
	}

	return packet2Write

}
