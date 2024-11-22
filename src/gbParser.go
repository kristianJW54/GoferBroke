package src

import "log"

// Thank the lord for NATS for being my spirit guide on this packet parsing journey - their scriptures are profound

const (
	START = iota
	INFO

	MSG_PAYLOAD
	MSG_PAYLOAD_NO_SIZE
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
	command      uint8
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
			case 'I':
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
				//if err := c.processINFO(arg); err != nil {
				//	log.Println("error processing info header:", err)
				//}
				log.Println("arg -->", string(arg))
				log.Println("arg as byte --> ", arg)

				c.drop = 0
				c.position = i + 1
				state = MSG_PAYLOAD_NO_SIZE

				//switch c.nh.msgLength {
				//case 0:
				//	log.Println("no message to process")
				//	state = START
				//case -1:
				//	log.Println("parsing unknown message size")
				//	state = MSG_PAYLOAD_NO_SIZE
				//default:
				//	log.Printf("expecting payload of %d", c.nh.msgLength)
				//	state = MSG_PAYLOAD
				//}

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
				//TODO finish from here
			}

		case MSG_PAYLOAD_NO_SIZE:

			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var msg []byte
				if c.msgBuf != nil {
					msg = make([]byte, len(c.msgBuf)+len(packet[c.position:i-c.drop]))
					copy(msg, c.msgBuf)
					copy(msg[len(c.msgBuf):], packet[c.position:i-c.drop])
					c.msgBuf = nil
				} else {
					msg = packet[c.position : i-c.drop]
				}

				log.Printf("completed message --> %s", string(msg))
				c.drop = 0
				c.position = i + 1
			}

		}

	}

}
