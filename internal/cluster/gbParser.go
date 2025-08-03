package cluster

import (
	"encoding/binary"
	"fmt"
)

const (
	NEW_JOIN = iota + 2
	SELF_INFO
	DISCOVERY_REQ
	DISCOVERY_RES // Discovery is acknowledging and responding with as many nodes and addresses as possible to onboard the node
	HANDSHAKE
	HANDSHAKE_RESP
	GOSS_SYN
	GOSS_SYN_ACK
	GOSS_ACK

	CFG_CHECK
	CFG_RECON
	CFG_RECON_RESP

	PROBE
	PROBE_RESP
	PING_CMD
	PONG_CMD

	OK
	OK_RESP
	EOS
	ERR_R
	ERR_RESP
)

type parserStateMachine int

const (
	Start parserStateMachine = iota
	ParsingHeader
	ParsingPayload
	StateREnd
	StateNEnd
	ParseClientCommand
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
		fmt.Printf("header length mismatch - got %v, expected %v\n", int(c.ph.headerLength), len(header))
		return
	}

	c.argBuf = header
	return

}

func (c *gbClient) ParsePacket(packet []byte) {

	var rounds int
	var b byte

	rounds++
	var i int

	//log.Printf("packet = %v", packet)

	for i = 0; i < len(packet); i++ {

		b = packet[i]

		switch c.state {
		case Start:
			switch b {
			case 1:
				//log.Printf("position = %v -- b = %v", c.position, b)
				c.position = i
				c.state = ParsingHeader
			default:
				c.position = i
				c.state = ParseClientCommand
			}

		case ParsingHeader:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':

				// Do a check here as we may encounter a situation where a command of [10] is encountered and misinterpreted as '\n'
				// Quick check of the previous byte will tell us
				if i == 0 || packet[i-1] != '\r' {
					continue
				}

				var header []byte
				if c.argBuf != nil {
					header = c.argBuf
					c.argBuf = nil
				} else {
					if c.position > i-c.drop {
						fmt.Printf("invalid header range: position=%d, i=%d, drop=%d\n", c.position, i, c.drop)
						c.state = Start
						c.argBuf = nil
						c.drop = 0
						continue
					}
					header = packet[c.position : i-c.drop]
				}

				//log.Printf("bytes passed to header %v", header)
				c.ProcessHeaderFromParser(header)

				// Reset drop byte
				c.drop = 0
				// Update position - we are currently on '\n'
				c.position = i + 1

				// Assume we have parsed a complete and can move to next state
				//log.Printf("switching to payload")
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
				c.srv.logger.Info("nope 1 -- b", "=", b)
				return
			}

			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			}

			c.state = StateNEnd

		case StateNEnd:

			if b != '\n' {
				c.srv.logger.Info("nope 2 -- b", "=", b)
				return
			}

			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			} else {
				c.msgBuf = packet[c.position : i+1]
			}

			//fmt.Printf("msg buf = %v\n", c.msgBuf)

			c.processMessage(c.msgBuf)

			c.argBuf, c.msgBuf = nil, nil
			c.ph.msgLength, c.ph.headerLength, c.ph.command, c.ph.version = 0, 0, 0, 0
			c.state = Start
			c.position = i + 1
			c.drop = 0

		case ParseClientCommand:

			if b == '\r' {
				c.drop = 1
			}
			if b == '\n' && c.drop == 1 {
				// End of command reached
				cmd := packet[c.position : i-c.drop] // Exclude CRLF
				c.dispatchClientCommands(cmd)

				// Reset state
				c.state = Start
				c.argBuf = nil
				c.drop = 0
				c.position = i + 1

			}

		}

	}

	// Process split buffer or scratch overflow here

	if (c.state == ParsingPayload || c.state == StateNEnd || c.state == StateREnd) && c.msgBuf == nil {

		if int(c.ph.msgLength) > cap(c.scratch)-(len(c.argBuf)) {

			rem := len(packet[c.position:])

			if rem > int(c.ph.msgLength)+len(CLRF) {
				fmt.Printf("error in parser\n")
				return
			}

			c.msgBuf = make([]byte, rem, int(c.ph.msgLength)+len(CLRF))
			copy(c.msgBuf, packet[c.position:])
		} else {
			c.msgBuf = c.scratch[len(c.argBuf):len(c.argBuf)]
			c.msgBuf = append(c.msgBuf, packet[c.position:]...)
		}

	}

	return

}
