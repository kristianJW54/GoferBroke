package main

import (
	"net"
	"testing"
	"time"
)

//TODO refactor test to update the status only while the server is accepting connections
// if the server connects but closes then the status needs to be closed

func TestServerRunning(t *testing.T) {

	lc := net.ListenConfig{}

	gbs := NewServer("test-server", "localhost", "8081", lc)

	go gbs.StartServer()
	//
	time.Sleep(8 * time.Second)
	//
	go gbs.Shutdown()

	//fmt.Printf("Server Name: %s \nResult: %s\n", gbs.ServerName, gbs.Status())

}
