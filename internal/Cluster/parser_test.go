package Cluster

import (
	"log"
	"testing"
)

var testGSA = []byte{
	1, 9, 0, 2, 0, 2, 0, 100, 0, 12, 13, 10, // Header + \r\n
	1, 0, 0, 0, 100, 0, 2, 24, 116, 101, 115, 116, 45, 115, 101, 114,
	118, 101, 114, 45, 50, 64, 49, 55, 52, 52, 55, 57, 49, 48, 52, 51,
	24, 116, 101, 115, 116, 45, 115, 101, 114, 118, 101, 114, 45, 49,
	64, 49, 55, 52, 52, 55, 57, 49, 48, 52, 50, 0, 0, 0, 0, 103, 255,
	102, 2, 24, 116, 101, 115, 116, 45, 115, 101, 114, 118, 101, 114,
	45, 50, 64, 49, 55, 52, 52, 55, 57, 49, 48, 52, 51, 0, 0, 0, 0,
	103, 255, 102, 5, 13, 10, // payload + \r\n
}

func TestParser(t *testing.T) {

	p := parser{
		state:    Start,
		position: 0,
		drop:     0,
		argBuf:   make([]byte, 0),
	}

	c := &gbClient{
		parser: p,
	}

	log.Printf("FULL LENGTH = %v", len(testGSA))

	testPacket := testGSA

	// Simulate packet splits:
	// 1. Partial header
	// 2. Rest of header + beginning of payload
	// 3. Remaining payload + footer (\r\n)
	broken1 := testPacket[:6]   // partial header
	broken2 := testPacket[6:36] // rest of header + part of payload
	broken3 := testPacket[36:]  // rest of payload + \r\n

	packets := [][]byte{broken1, broken2, broken3}

	for i, packet := range packets {
		log.Printf("---- PACKET %d ----", i)
		c.ParsePacket(packet)
		log.Printf("state --> %v", c.parser.state)
	}
}
