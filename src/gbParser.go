package src

import "log"

const (
	START = iota
	INFO
)

type stateMachine struct {
	state    int
	parsed   int
	position int
	drop     int
	b        byte
	args     []byte
	msg      []byte
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
				// TODO continue from here and finish
			}

		}

	}

}
