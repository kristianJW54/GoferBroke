package src

const (
	START = iota
	VERSION1

	// Node Commands
	NEW_JOIN
	SELF_INFO
	DISCOVERY_REQ
	DISCOVERY_RES // Discovery is acknowledging and responding with as many nodes and addresses as possible to onboard the node
	HANDSHAKE
	HANDSHAKE_RESP
	GOSS_SYN
	GOSS_SYN_ACK
	GOSS_ACK
	TEST

	// Client Commands
	DELTA
	DELTA_KEY

	// Message End
	MSG_PAYLOAD
	MSG_R_END
	MSG_N_END

	// Response types
	OK
	OK_RESP
	EOS
	ERR_RESP
)

type parserState int
type stateMachine struct {
	state    parserState
	parsed   int
	position int
	drop     int
	command  byte
	argBuf   []byte
	msgBuf   []byte
	ph       parseHeader

	scratch [4096]byte

	// Testing stats
	rounds int
}

// parseHeader is used by parsing handlers when parsing argBuf to populate and hold the node header
type parseHeader struct {
	version      uint8
	command      uint8
	reqID        uint16
	respID       uint16
	keyLength    int
	valueLength  int
	msgLength    int
	headerLength int
}

func (c *gbClient) parsePacket(packet []byte) {

	c.rounds++

	var i int
	var b byte

	for i = 0; i < len(packet); i++ {

		b = packet[i]

		switch c.state {
		case START:
			c.command = b
			switch b {
			case 1:
				c.position = i
				c.state = VERSION1
			case 'V':
				c.position = i
				c.state = DELTA
				//log.Printf("ROUND %d START = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
			}

		case DELTA:
			switch b {
			case '\r':
				c.drop = 1
				//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
				c.drop = 1
			case '\n':
				if packet[i-1] == 13 {
					//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
					var arg []byte
					if c.argBuf != nil {
						arg = c.argBuf
						c.argBuf = nil
					} else {
						arg = packet[c.position : i-c.drop]
					}
					c.processDeltaHdr(arg)

					c.drop = 0
					c.position = i + 1
					c.state = MSG_PAYLOAD

					if c.msgBuf == nil {
						i = c.position + c.ph.msgLength - 2
					}
				}
			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
					//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
				}
				//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
			}

		case VERSION1:
			switch b {
			case NEW_JOIN:
				c.state = NEW_JOIN
			case SELF_INFO:
				c.state = SELF_INFO
			case OK:
				c.state = OK
			case OK_RESP:
				c.state = OK_RESP
			case DISCOVERY_REQ:
				c.state = DISCOVERY_REQ
			case DISCOVERY_RES:
				c.state = DISCOVERY_RES
			case HANDSHAKE:
				c.state = HANDSHAKE
			case HANDSHAKE_RESP:
				c.state = HANDSHAKE_RESP
			case GOSS_SYN:
				c.state = GOSS_SYN
			case ERR_RESP:
				c.state = ERR_RESP
			}

		case NEW_JOIN:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = packet[c.position : i-c.drop]
				}
				c.processArg(arg)

				c.drop = 0
				c.position = i + 1
				c.state = MSG_PAYLOAD

				if c.msgBuf == nil {
					i = c.position + c.ph.msgLength - 2
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case SELF_INFO:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = packet[c.position : i-c.drop]
				}
				c.processArg(arg)

				c.drop = 0
				c.position = i + 1
				c.state = MSG_PAYLOAD

				if c.msgBuf == nil {
					i = c.position + c.ph.msgLength - 2
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case OK:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = packet[c.position : i-c.drop]
				}
				c.processArg(arg)

				c.drop = 0
				c.position = i + 1
				c.state = MSG_PAYLOAD

				if c.msgBuf == nil {
					i = c.position + c.ph.msgLength - 2
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case OK_RESP:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = packet[c.position : i-c.drop]
				}
				c.processArg(arg)

				c.drop = 0
				c.position = i + 1
				c.state = MSG_PAYLOAD

				if c.msgBuf == nil {
					i = c.position + c.ph.msgLength - 2
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case ERR_RESP:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = packet[c.position : i-c.drop]
				}
				c.processArg(arg)

				c.drop = 0
				c.position = i + 1
				c.state = MSG_PAYLOAD

				if c.msgBuf == nil {
					i = c.position + c.ph.msgLength - 2
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case DISCOVERY_REQ:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				if packet[i-1] == 13 {
					//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
					var arg []byte
					if c.argBuf != nil {
						arg = c.argBuf
						c.argBuf = nil
					} else {
						arg = packet[c.position : i-c.drop]
					}
					c.processArg(arg)

					c.drop = 0
					c.position = i + 1
					c.state = MSG_PAYLOAD

					if c.msgBuf == nil {
						i = c.position + c.ph.msgLength - 2
					}
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case DISCOVERY_RES:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				if packet[i-1] == 13 {
					//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
					var arg []byte
					if c.argBuf != nil {
						arg = c.argBuf
						c.argBuf = nil
					} else {
						arg = packet[c.position : i-c.drop]
					}
					c.processArg(arg)

					c.drop = 0
					c.position = i + 1
					c.state = MSG_PAYLOAD

					if c.msgBuf == nil {
						i = c.position + c.ph.msgLength - 2
					}
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case HANDSHAKE:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				if packet[i-1] == 13 {
					//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
					var arg []byte
					if c.argBuf != nil {
						arg = c.argBuf
						c.argBuf = nil
					} else {
						arg = packet[c.position : i-c.drop]
					}
					c.processArg(arg)

					c.drop = 0
					c.position = i + 1
					c.state = MSG_PAYLOAD

					if c.msgBuf == nil {
						i = c.position + c.ph.msgLength - 2
					}
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case HANDSHAKE_RESP:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				if packet[i-1] == 13 {
					//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
					var arg []byte
					if c.argBuf != nil {
						arg = c.argBuf
						c.argBuf = nil
					} else {
						arg = packet[c.position : i-c.drop]
					}
					c.processArg(arg)

					c.drop = 0
					c.position = i + 1
					c.state = MSG_PAYLOAD

					if c.msgBuf == nil {
						i = c.position + c.ph.msgLength - 2
					}
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case GOSS_SYN:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				if packet[i-1] == 13 {
					//log.Printf("ROUND %d DELTA = i: %d, position: %d --> b = %v %s\n", c.rounds, i, c.position, b, string(b))
					var arg []byte
					if c.argBuf != nil {
						arg = c.argBuf
						c.argBuf = nil
					} else {
						arg = packet[c.position : i-c.drop]
					}
					c.processArg(arg)

					c.drop = 0
					c.position = i + 1
					c.state = MSG_PAYLOAD

					if c.msgBuf == nil {
						i = c.position + c.ph.msgLength - 2
					}
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case MSG_PAYLOAD:
			if c.msgBuf != nil {
				left := c.ph.msgLength - len(c.msgBuf)
				avail := len(c.msgBuf) - i
				if avail < left {
					left = avail
				}
				if left > 0 {
					start := len(c.msgBuf)
					c.msgBuf = c.msgBuf[:start+left]
					copy(c.msgBuf[start:], packet[i:i+left])
					i = (i + left) - 1
				} else {
					c.msgBuf = append(c.msgBuf, b)
				}

				if len(c.msgBuf) >= c.ph.msgLength {
					i = i - 2
					c.state = MSG_R_END
				}
			} else if i-c.position+1 >= c.ph.msgLength {
				i = i - 2
				c.state = MSG_R_END
			}

		case MSG_R_END:
			if b != '\r' {
				return
			}
			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			}
			c.state = MSG_N_END

		case MSG_N_END:
			if b != '\n' {
				return
			}
			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			} else {
				c.msgBuf = packet[c.position : i+1]
			}

			//log.Printf("%s = final message --> %s\n\n", c.srv.ServerName, string(c.msgBuf))

			c.processMessage(c.msgBuf)

			c.argBuf, c.msgBuf = nil, nil
			c.ph.msgLength, c.ph.headerLength, c.ph.command, c.ph.version = 0, 0, 0, 0
			c.state = START
			c.position = i + 1
			c.drop = 0

			c.rounds = 0
		}
	}

	if c.state == MSG_PAYLOAD || c.state == MSG_R_END && c.msgBuf == nil {
		if c.ph.msgLength > cap(c.scratch)-len(c.argBuf) {
			rem := len(packet[c.position:])
			if rem > c.ph.msgLength+2 {
				return
			}

			c.msgBuf = make([]byte, rem, c.ph.msgLength+2)
			copy(c.msgBuf, packet[c.position:])
		} else {
			c.msgBuf = c.scratch[len(c.argBuf):len(c.argBuf)]
			c.msgBuf = append(c.msgBuf, packet[c.position:]...)
		}
	}

	return
}
