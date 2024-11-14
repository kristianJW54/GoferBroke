package src

import "net"

type gbClient struct {
	srv *GBServer

	gbc     net.Conn
	inbound readCache
}

type readCache struct {
	buffer      []byte
	offset      int
	expandCount int
	buffSize    int
}
