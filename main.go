package main

import (
	"fmt"
	"net"
)

func main() {

	fmt.Println("===================================================")
	fmt.Println("                   GoferBrokeMQ                    ")
	fmt.Println("===================================================")

	//==============================================================

	tcp, err := net.Listen("tcp", "localhost:8081")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer tcp.Close()

	fmt.Printf("Listening on %s\n", tcp.Addr())

	//TODO set up basic message protocal structure
	//TODO implement basic client dial
	//TODO implement basic keepalive with timeout and idle connection timeout

}
