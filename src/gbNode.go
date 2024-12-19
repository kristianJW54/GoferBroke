package src

import (
	"encoding/binary"
	"fmt"
	"log"
)

//===================================================================================
// Handlers
//===================================================================================

func (c *gbClient) processINFO(arg []byte) error {
	// Assuming the first 3 bytes represent the command and the next bytes represent msgLength

	if len(arg) >= 4 {
		c.nh.version = arg[0]
		c.nh.id = arg[2]
		c.nh.command = arg[1]
		// Extract the last 4 bytes
		msgLengthBytes := arg[3:5]
		// Convert those 4 bytes to uint32 (BigEndian)
		c.nh.msgLength = int(binary.BigEndian.Uint16(msgLengthBytes))

		// Log the result to verify
		//log.Printf("Extracted msgLength: %d\n", c.nh.msgLength)
	} else {
		return fmt.Errorf("argument does not have enough bytes to extract msgLength")
	}

	c.argBuf = arg

	return nil
}

//---------------------------
// Node Handlers

func (c *gbClient) dispatchNodeCommands(message []byte) {

	//GOSS_SYN
	//GOSS_SYN_ACK
	//GOSS_ACK
	//TEST

	switch c.nh.command {
	case INFO:
		c.processInitialMessage(message)
	case GOSS_SYN:
		c.processGossSyn(message)
	case GOSS_SYN_ACK:
		c.processGossSynAck(message)
	case GOSS_ACK:
		c.processGossAck(message)
	case OK:
		c.processOK(message)
	case ERR_RESP:
		c.processErrResp(message)
	default:
		log.Printf("unknown command %v", c.nh.command)
	}

}

func (c *gbClient) processErrResp(message []byte) {

}

func (c *gbClient) processOK(message []byte) {

	//log.Printf("returned message = %s", string(message))
	c.rm.Lock()
	responseChan, exists := c.resp[int(c.argBuf[2])]
	c.rm.Unlock()

	if exists {

		responseChan <- message

		c.mu.Lock()
		delete(c.resp, int(c.argBuf[0]))
		c.mu.Unlock()

	} else {
		log.Printf("no response channel found")
	}

}

func (c *gbClient) processGossAck(message []byte) {

}

func (c *gbClient) processGossSynAck(message []byte) {

}

func (c *gbClient) processGossSyn(message []byte) {

}

func (c *gbClient) processInitialMessage(message []byte) {

	tmpC, err := deserialiseDelta(message)
	if err != nil {
		log.Printf("deserialiseDelta failed: %v", err)
	}
	for key, value := range tmpC.delta {
		log.Printf("key = %s", key)
		for k, v := range value.keyValues {
			log.Printf("value[%v]: %v", k, v)
		}
	}

	// TODO - node should check if message is of correct info - add to it's own cluster map and then respond

	cereal := []byte("OK +\r\n")

	// Construct header
	header := constructNodeHeader(1, OK, 1, uint16(len(cereal)), NODE_HEADER_SIZE_V1)
	// Create packet
	packet := &nodePacket{
		header,
		cereal,
	}
	pay1, _ := packet.serialize()

	c.qProto(pay1, true)

}
