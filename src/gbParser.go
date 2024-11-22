package src

import "log"

// Thank the lord for NATS for being my spirit guide on this packet parsing journey - their scriptures are profound

const (
	START = iota
	INFO

	MSG_PAYLOAD
)

type stateMachine struct {
	state    int
	parsed   int
	position int
	drop     int
	b        byte
	argBuf   []byte
	msgBuf   []byte
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
				if err := c.processINFO(arg); err != nil {
					log.Println("error processing info header:", err)
				}

				c.drop = 0
				c.position = i + 1
				state = MSG_PAYLOAD

				// If msg buf is nil then it means we continue
				if c.msgBuf == nil {
					i = c.position - 2 //len CRLF
				}

			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
				//TODO finish from here
			}

		case MSG_PAYLOAD:

			switch b {

			}

		}

	}

}
