package src

import "log"

// Thank the lord for NATS for being my spirit guide on this packet parsing journey - their scriptures are profound

const (
	START = iota
	INFO

	MSG_PAYLOAD
	MSG_R_END
	MSG_N_END
)

type stateMachine struct {
	state    int
	parsed   int
	position int
	drop     int
	b        byte
	argBuf   []byte
	msgBuf   []byte
	nh       nodeHeader
}

type nodeHeader struct {
	version      uint8
	command      uint32
	msgLength    int
	headerLength int
}

func (c *gbClient) parsePacket(packet []byte) {

	i := c.position
	b := c.b
	state := c.state

	for i = 0; i < len(packet); i++ {

		b = packet[i]

		switch state {
		case START:

			switch b {
			case 1:
				log.Println("switching to info state")
				state = INFO
			default:
				log.Println("something wrong with yo state fam")
			}

		case INFO:

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
				// TODO Make this method
				if err := c.processINFO(arg); err != nil {
					log.Println("error processing info header:", err)
				}

				c.drop = 0
				c.position = i + 1
				log.Println("position after info : ", c.position)
				log.Println("i after info: ", i)
				log.Println("switching to message payload state")
				state = MSG_PAYLOAD

				//if c.msgBuf == nil {
				//	// If no saved buffer exists, assume this packet contains the full payload.
				//	// Calculate the position to jump directly to the end of the payload.
				//	i = c.position + c.nh.msgLength - 2 // Subtract 2 for CRLF at the end.
				//	log.Println("No saved buffer. Skipping to i:", i)
				//}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case MSG_PAYLOAD:

			if c.msgBuf != nil {
				log.Println(string(c.msgBuf))
				left := c.nh.msgLength - len(c.msgBuf)
				avail := len(c.msgBuf) - i
				if avail < left {
					left = avail
				}
				if left > 0 {
					start := len(c.msgBuf)
					c.msgBuf = c.msgBuf[start : start+left]
					copy(c.msgBuf[i:], packet[i:i+left])
					i = (i + left) - 1
				} else {
					c.msgBuf = append(c.msgBuf, b)
				}

				if len(c.msgBuf) >= c.nh.msgLength {
					state = MSG_R_END
				}
			} else if i-c.position+1 >= c.nh.msgLength {
				state = MSG_R_END
			}

		case MSG_R_END:
			if b != '\r' {
				log.Println("end of message error")
				return
			}
			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			}
			state = MSG_N_END
		case MSG_N_END:
			if b != '\n' {
				log.Println("end of message error")
				return
			}
			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			} else {
				c.msgBuf = packet[c.position : i+1]
			}
		}
	}
}
