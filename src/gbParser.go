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
	command  byte
	argBuf   []byte
	msgBuf   []byte
	nh       nodeHeader

	scratch [4096]byte

	//Testing stats
	rounds int
}

type nodeHeader struct {
	version      uint8
	command      uint32
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

			log.Println("c.parsed ", c.parsed)
			log.Println("position ", c.position)
			log.Println("command", c.command)
			log.Println("packet - ", len(packet))
			log.Println("arg - ", c.argBuf)
			log.Println("msg - ", c.msgBuf)

			c.command = b

			switch b {
			case 1:
				log.Println("switching to info state")
				c.position = i // Important to reset according to i if we enter a new header to avoid slice error
				c.state = INFO
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
				c.state = MSG_PAYLOAD

				if c.msgBuf == nil {
					// If no saved buffer exists, assume this packet contains the full payload.
					// Calculate the position to jump directly to the end of the payload.
					i = c.position + c.nh.msgLength - 2 // Subtract 2 for CRLF at the end.
					log.Println("No saved buffer. Skipping to i:", i)
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}

		case MSG_PAYLOAD:

			if c.msgBuf != nil {
				left := c.nh.msgLength - len(c.msgBuf)
				log.Println("what is left to copy --> ", left)
				avail := len(c.msgBuf) - i
				if avail < left {
					left = avail
				}
				if left > 0 {
					start := len(c.msgBuf)
					log.Println("start --> ", start)
					log.Println("length of msg.buf before: ", len(c.msgBuf))
					c.msgBuf = c.msgBuf[:start+left]
					log.Println("length of msg.buf after: ", len(c.msgBuf))
					copy(c.msgBuf[start:], packet[i:i+left])
					log.Println("msg buf after copy --> ", c.msgBuf)
					i = (i + left) - 1
					//TODO look at i -- for some reason - 1 works but the loop is exiting because its at the end of the index
					// need to fix in order to reach message end
					log.Println("i -- ", i)
				} else {
					c.msgBuf = append(c.msgBuf, b)
				}

				if len(c.msgBuf) >= c.nh.msgLength {
					log.Println("switching to r_end 1")
					i = i - 2
					log.Println("message --> ", c.msgBuf)
					log.Println("position > ", c.position)
					log.Println("i > ", i)
					log.Println("next byte -- ", packet[i])
					log.Println(len(packet))
					c.state = MSG_R_END
				}
			} else if i-c.position+1 >= c.nh.msgLength {
				log.Println("switching to r_end 2")
				i = i - 2
				c.state = MSG_R_END
			}

		case MSG_R_END:
			log.Println("arrived at r_end")
			log.Println(c.msgBuf)
			if b != '\r' {
				log.Println("end of message error")
				log.Println("printing b ", b)
				return
			} else {
				log.Println("printing b ", b)
			}
			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			}
			log.Println("switching to n_end")
			c.state = MSG_N_END
		case MSG_N_END:
			if b != '\n' {
				log.Println("end of message error")
			}
			if c.msgBuf != nil {
				c.msgBuf = append(c.msgBuf, b)
			} else {
				c.msgBuf = packet[c.position : i+1]
			}
			log.Println("n_end")
			log.Println(c.msgBuf)
			log.Println("final message --> ", string(c.msgBuf))
			c.argBuf, c.msgBuf = nil, nil
			c.nh.msgLength, c.nh.headerLength, c.nh.command, c.nh.version = 0, 0, 0, 0
			c.state = START
			c.position = i + 1
			c.drop = 0
		}
	}

	//TODO we are resetting the buffer each time here - we need to not

	log.Println("end of for loop - starting again lol")
	log.Println("state = ", c.state)
	if c.state == MSG_PAYLOAD || c.state == MSG_R_END && c.msgBuf == nil {

		if c.nh.msgLength > cap(c.scratch)-len(c.argBuf) {
			rem := len(packet[c.position:])
			if rem > c.nh.msgLength+2 {
				log.Println("cap error")
				return
			}

			c.msgBuf = make([]byte, rem, c.nh.msgLength+2)
			copy(c.msgBuf, packet[c.position:])
		} else {
			c.msgBuf = c.scratch[len(c.argBuf):len(c.argBuf)]
			c.msgBuf = append(c.msgBuf, packet[c.position:]...)
		}

		log.Println("printing scratch : ", c.scratch)

		// Do check if remaining is greater than expected

		//Take what we have and store it
		//newBuf := make([]byte, len(packet[c.position:]), c.nh.msgLength)
		//copy(newBuf, packet[c.position:])
		//c.msgBuf = newBuf
		//log.Println("msg buf after copy: ", len(c.msgBuf))
		//log.Println("msg buf == ", c.msgBuf)

	}

	return

}
