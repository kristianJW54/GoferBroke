package src

import (
	"net"
	"sync"
	"time"
)

const (
	defaultMaxPoolSize = 100
)

type GbConn struct {
	gbc      *net.TCPConn
	lastUsed time.Time

	gbcWG *sync.WaitGroup
}
