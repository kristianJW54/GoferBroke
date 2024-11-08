package src

import (
	"net"
	"sync"
)

type GbConn struct {
	gbc        net.Conn
	clientType int

	gbcWG *sync.WaitGroup
}

//TODO Think about if i want to have read loop and write loops here - binary marshalling etc
