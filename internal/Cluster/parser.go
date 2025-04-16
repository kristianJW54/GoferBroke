package Cluster

import (
	"encoding/binary"
	"log"
)

type parserStateMachine int

const (
	Start parserStateMachine = iota
	ParsingHeader
	ParsingPayload
	StateREnd
	StateNEnd
)

type parser struct {
	state    parserStateMachine
	position int
	drop     int
	parsed   int
	ph       parsedHeader
	argBuf   []byte
	msgBuf   []byte
	scratch  [4096]byte
}

type parsedHeader struct {
	version       uint8
	command       uint8
	reqID         uint16
	respID        uint16
	keyLength     int
	valueLength   int
	payloadLength uint16
	msgLength     uint16
	headerLength  uint16
}

func (c *gbClient) ProcessHeaderFromParser(header []byte) {

	//log.Printf("bytes passed to header %v", header)

	if len(header) > 4 {
		c.ph.version = header[0]
		c.ph.command = header[1]
		c.ph.reqID = binary.BigEndian.Uint16(header[2:4])
		c.ph.respID = binary.BigEndian.Uint16(header[4:6])
		// Message length
		msgLength := header[6:8]
		c.ph.msgLength = binary.BigEndian.Uint16(msgLength) - 2
		// Header length
		//headerLength := header[8:10]
		//c.header.headerLength = binary.BigEndian.Uint16(headerLength) - 2

	} else {
		log.Printf("header length mismatch - got %v, expected %v", int(c.ph.headerLength), len(header))
		return
	}

	c.argBuf = header
	return

}

var rounds int
var b byte

func (c *gbClient) ParsePacket(packet []byte) {

	rounds++

	for i := 0; i < len(packet); i++ {

		b = packet[i]

		if c.state == Start && c.position > 0 {
			c.position = 0
		}

		switch c.state {
		case Start:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':

				var header []byte
				if c.argBuf != nil {
					header = c.argBuf
					c.argBuf = nil
				} else {
					header = packet[c.position : i-c.drop]
				}

				c.ProcessHeaderFromParser(header)

				// Reset drop byte
				c.drop = 0
				// Update position - we are currently on '\n'
				c.position = i + 1

				// Assume we have parsed a complete and can move to next state
				log.Printf("switching to payload")
				c.state = ParsingPayload

				// Check if we can skip byte index with msgLength
				if c.msgBuf == nil {
					i = int(c.position) + int(c.ph.msgLength) - len(CLRF)
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case ParsingPayload:

			//log.Printf("i = %v -- position = %v -- msgLen = %v", i, c.position, c.header.msgLength)

			if c.msgBuf != nil {
				// accumulate (split case)

				left := int(c.ph.msgLength) - len(c.msgBuf)
				remaining := len(packet) - i

				// If what is remaining in the packet is less than what we have left to parse
				// make what is left the same as remaining to accumulate the remainder of the packet
				// we do this until we have nothing left
				if remaining < left {
					left = remaining
				}

				if left > 0 {
					start := len(c.msgBuf)
					c.msgBuf = c.msgBuf[:start+left]
					copy(c.msgBuf[start:], packet[i:i+left])
					i = (i + left) - 1
				} else {
					c.msgBuf = append(c.msgBuf, b)
				}

				if len(c.msgBuf) >= int(c.ph.msgLength) {
					c.state = StateREnd
				}

			} else if i-c.position+1 >= int(c.ph.msgLength) {
				c.state = StateREnd
			}

		case StateREnd:

			if b != '\r' {
				log.Printf("nope -- b = %v", b)
				return
			}

			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			}

			c.state = StateNEnd

		case StateNEnd:

			if b != '\n' {
				log.Printf("nope -- b = %v", b)
				return
			}

			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			} else {
				c.msgBuf = packet[c.position : i+1]
			}

			//log.Printf("msg buf = %v", c.msgBuf)

			c.processMessage(c.msgBuf)

			c.argBuf, c.msgBuf = nil, nil
			c.ph.msgLength, c.ph.headerLength, c.ph.command, c.ph.version = 0, 0, 0, 0
			c.state = Start
			c.position = i + 1
			c.drop = 0

		}

	}

	// Process split buffer or scratch overflow here

	if (c.state == ParsingPayload || c.state == StateNEnd || c.state == StateREnd) && c.msgBuf == nil {

		if int(c.ph.msgLength) > cap(c.scratch)-(len(packet)-c.position) {

			rem := len(packet[c.position:])

			if rem > int(c.ph.msgLength)+len(CLRF) {
				log.Printf("error in parser")
				return
			}

			c.msgBuf = make([]byte, rem, int(c.ph.msgLength)+len(CLRF))
			copy(c.msgBuf, packet[c.position:])
		} else {
			c.msgBuf = c.scratch[len(c.argBuf):len(c.argBuf)]
			c.msgBuf = append(c.msgBuf, packet[c.position:]...)
		}

	}

}
