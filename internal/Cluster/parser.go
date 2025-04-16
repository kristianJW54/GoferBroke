package Cluster

import "log"

type parserStateMachine int

const (
	Start parserStateMachine = iota
	ParsingHeader
	ParsingPayload
	StateREnd
	StateNEnd
)

type parseMeta struct {
	state   parserStateMachine
	parsed  int
	header  parsedHeader
	argBuf  []byte
	msgBuf  []byte
	scratch [4096]byte
}

type parsedHeader struct {
	version       uint8
	command       uint8
	reqID         uint16
	respID        uint16
	payloadLength uint16
	msgLength     uint16
	headerLength  uint16
}

func (pm *parseMeta) ProcessHeaderFromParser(header []byte) error {

	return nil

}

var rounds int
var b byte

func (pm *parseMeta) ParsePacket(packet []byte) {

	rounds++

	pm.state = Start

	for i := 0; i < len(packet); i++ {

		b = packet[i]

		switch pm.state {
		case Start:
			switch b {
			case '\r':
				log.Printf("packet until now = %v", packet[:i])
				return
			}
		}

	}

}
