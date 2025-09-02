package cluster

import (
	"bytes"
	"fmt"
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

var split1 = []byte{
	1, 9, 0, 1, 0, 3, 0, 100, 0, 12, 13, 10, 1, 0, 0, 0, 100, 0, 2, 24, 116, 101, 115, 116, 45, 115,
	101, 114, 118, 101, 114, 45, 50, 64, 49, 55, 52, 52, 56, 55, 54, 54, 53, 49, 24, 116, 101, 115,
	116, 45, 115, 101, 114, 118, 101, 114, 45, 49, 64, 49, 55, 52, 52, 56, 55, 54, 54, 53, 48, 0, 0,
	0, 0, 104, 0, 180, 106, 24, 116, 101, 115, 116, 45, 115, 101, 114, 118, 101, 114, 45, 50, 64, 49,
	55, 52, 52, 56, 55, 54, 54, 53, 49, 0, 0, 0, 0, 104, 0, 180, 109, 13, 10, 1, 10, 0, 0, 0, 3, 0,
	205, 0, 12, 13, 10, 2, 0, 0, 0, 205, 0, 1, 24, 116, 101, 115, 116, 45, 115, 101, 114, 118, 101,
	114, 45, 50, 64, 49, 55, 52, 52, 56, 55, 54, 54, 53, 49, 24, 116, 101, 115, 116, 45, 115, 101,
	114, 118, 101, 114, 45, 50, 64, 49, 55, 52, 52, 56, 55, 54, 54, 53, 49, 0, 4, 7, 97, 100, 100,
	114, 101, 115, 115, 3, 116, 99, 112, 0, 0, 0, 0, 104, 0, 180, 107, 2, 0, 0, 0, 14, 108, 111, 99,
	97, 108, 104, 111, 115, 116, 58, 56, 48, 56, 50, 7, 110, 101, 116, 119, 111, 114, 107, 12, 114,
	101, 97, 99, 104, 97, 98, 105, 108, 105, 116, 121, 0, 0, 0, 0, 104, 0, 180, 107, 2, 0, 0, 0, 1,
}

var split2 = []byte{
	1, 6, 115, 121, 115, 116, 101, 109, 10, 110, 111, 100, 101, 95, 99, 111, 110, 110, 115, 0, 0, 0,
	0, 104, 0, 180, 107, 2, 0, 0, 0, 1, 0, 6, 115, 121, 115, 116, 101, 109, 9, 104, 101, 97, 114, 116,
	98, 101, 97, 116, 0, 0, 0, 0, 104, 0, 180, 109, 2, 0, 0, 0, 8, 0, 0, 0, 0, 104, 0, 180, 109, 13,
	10,
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

	testPacket := testGSA

	// Simulate packet splits:
	// 1. Partial header
	// 2. Rest of header + beginning of payload
	// 3. Remaining payload + footer (\r\n)
	broken1 := testPacket[:6]   // partial header
	broken2 := testPacket[6:36] // rest of header + part of payload
	broken3 := testPacket[36:]  // rest of payload + \r\n

	packets := [][]byte{broken1, broken2, broken3}

	for _, packet := range packets {
		c.ParsePacket(packet)
	}

	if c.msgBuf == nil {
		t.Errorf("should have parsed into msgBuf")
	}

	// Take header out of test packet
	if !bytes.Equal(testPacket[12:], c.msgBuf) {
		t.Errorf("test packet does not match msgBuf")
	}

}

func TestSplitPacketFromChunks(t *testing.T) {
	p := parser{
		state:    Start,
		position: 0,
		drop:     0,
		argBuf:   make([]byte, 0),
	}

	client := &gbClient{
		parser: p,
	}

	chunks := [][]byte{split1, split2}

	for _, chunk := range chunks {
		client.ParsePacket(chunk)
	}

	s := len(split1[12:])

	checkBuf := make([]byte, len(split1[12:])+len(split2))
	copy(checkBuf, split1[12:])
	copy(checkBuf[s:], split2)

	fmt.Println(checkBuf)

	if !bytes.Equal(checkBuf, client.msgBuf) {
		t.Errorf("split packets does not match msgBuf")
	}

}

func BenchmarkParseThroughput(b *testing.B) {
	p := parser{
		state:    Start,
		position: 0,
		drop:     0,
		argBuf:   make([]byte, 0),
	}

	client := &gbClient{
		parser: p,
	}

	// Simulate one big stream from real packet chunks
	stream := append([]byte{}, split1...)
	stream = append(stream, split2...)

	b.SetBytes(int64(len(stream))) // tell the benchmark how many bytes are processed per iteration

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		client.ParsePacket(stream)
	}
}
