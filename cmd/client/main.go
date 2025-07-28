package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func streamLogs(conn net.Conn, response *bufio.Reader, input *bufio.Reader) {
	fmt.Println("Streaming logs... type STOP_STREAM or press Ctrl+C to stop.")

	// Used to interrupt streaming from main thread or signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	stopCommand := make(chan struct{})
	done := make(chan struct{})

	// Log reading goroutine
	go func() {
		for {
			select {
			case <-stopCommand:
				close(done)
				return
			default:
				line, err := response.ReadString('\n')
				if err != nil {
					fmt.Printf("Log stream error: %v\n", err)
					close(done)
					return
				}
				fmt.Print(line)
			}
		}
	}()

	// User input monitor
	go func() {
		for {
			fmt.Print("> ")
			line, err := input.ReadString('\n')
			if err != nil {
				fmt.Printf("Input error during stream: %v\n", err)
				continue
			}
			if strings.TrimSpace(line) == "STOP_STREAM" {
				fmt.Fprint(conn, "STOP_STREAM\r\n")
				close(stopCommand)
				return
			}
		}
	}()

	// Block until Ctrl+C or STOP_STREAM
	select {
	case <-stop:
		fmt.Fprint(conn, "STOP_STREAM\r\n")
		close(stopCommand)
	case <-done:
	}

	fmt.Println("Stopped streaming logs.")
}

func run(at io.Writer, addr string) error {

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to connect to node at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	input := bufio.NewReader(os.Stdin)
	response := bufio.NewReader(conn)

	fmt.Printf("Connected to node at %s\n", addr)
	fmt.Println("Type 'help' for available commands.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})

	// Read incoming messages
	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		close(done)
	}()

	// Start interactive loop
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("input error:", err)
			break
		}
		cmd := strings.TrimSpace(line)
		switch cmd {
		case "exit", "quit":
			fmt.Println("Exiting.")
			return err
		case "help":
			printHelp()
			continue
		case "STREAM LOGS":

			fmt.Fprintf(conn, "STREAM_LOGS\r\n")
			// Start streaming mode
			streamLogs(conn, response, input)
			continue
		case "":
			continue
		}
		_, err = conn.Write([]byte(cmd + "\r\n"))
		if err != nil {
			fmt.Printf("write error: %v\n", err)
			return err
		}
	}

	<-done

	return nil

}

func printHelp() {
	fmt.Println(`
	Available Commands:
	  PING            - Ping the server, expect PONG.
	  STREAM_LOGS     - Begin log streaming.
	  exit / quit     - Close connection and exit.
	  help            - Show this message.
	`)
}

func main() {

	nodeAddr := flag.String("nodeAddr", "", "address of the node to connect to in the cluster")
	// Will add maybe admin, username, password, mTLS etc. - but would need to be separate client SDK

	flag.Parse()

	if *nodeAddr == "" {
		flag.Usage()
		os.Exit(1)
	}

	err := run(os.Stdout, *nodeAddr)
	if err != nil {
		fmt.Printf("%v,%v", os.Stderr, err)
		os.Exit(1)
	}

}
