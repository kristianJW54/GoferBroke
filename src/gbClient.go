package src

import (
	"io"
	"log"
	"net"
	"net/url"
)

const (
	CLIENT = iota
	NODE
)

//===================================================================================
// Client
//===================================================================================

type gbClient struct {
	Name string

	srv *GBServer

	gbc     net.Conn
	inbound readCache

	cType int

	//Routing info
	nodeUrl url.URL
}

//===================================================================================
// Read Cache
//===================================================================================

type readCache struct {
	buffer      []byte
	offset      int
	expandCount int
	buffSize    int
}

//===================================================================================
// Client creation
//===================================================================================

func (s *GBServer) createClient(conn net.Conn, name string, clientType int) *gbClient {

	log.Printf("creating connection --> %s --> type: %d\n", name, clientType)

	client := &gbClient{
		Name:  name,
		srv:   s,
		gbc:   conn,
		cType: clientType,
	}

	go func() {
		defer conn.Close()
		client.readLoop()
	}()

	return client

}

//===================================================================================
// Client Connection + Wire Handling
//===================================================================================

//---------------------------
//Read Loop

func (c *gbClient) readLoop() {

	// Read and parse inbound messages

	//Check locks and if anything is closed or shutting down

	buf := make([]byte, INITIAL_BUFF_SIZE)

	var reader io.Reader
	reader = c.gbc

	//TODO Look at implementing a specific read function to handle our TCP Packets and have
	// our own protocol specific rules around read

	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			log.Println("read error", err)
			return
		}
		if n == 0 {
			return
		}

		//TODO Implement a handler router for server-server connections and client-server connections
		// Similar to Nats where the read and write loop are run inside the handle (or in NATS case the connFunc)

		// Create a GossipPayload to unmarshal the received data into
		dataPayload := &TCPPacket{&PacketHeader{}, buf} //TODO This needs to a function to create a buffered payload

		err = dataPayload.UnmarshallBinaryV1(buf[:n]) // Read the exact number of bytes
		if err != nil {
			log.Println("unmarshall error", err)
			return
		}

		// Log the decoded data (as a string)
		log.Printf(
			"%v %v %v %v %s",
			dataPayload.Header.ProtoVersion,
			dataPayload.Header.Command,
			dataPayload.Header.MsgLength,
			dataPayload.Header.PayloadLength,
			string(dataPayload.Data),
		)
	}

}
