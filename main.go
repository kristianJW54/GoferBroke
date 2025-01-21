package main

import (
	"context"
	"flag"
	"fmt"
	src "goferMQ/src"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
)

func run(ctx context.Context, w io.Writer, name string, uuid int, clusterAddr string, nodeIp, nodePort string) error {
	// Create a new context that listens for interrupt signals
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	lc := net.ListenConfig{}

	var config *src.GbConfig

	if clusterAddr == "" {
		ip := "localhost" // Use the localhost for now - will change when actually config is implemented
		port := "8081"

		// Initialize config with the seed server address
		config = &src.GbConfig{
			SeedServers: []src.Seeds{
				{
					SeedIP:   ip,
					SeedPort: port,
				},
			},
		}
		log.Println("Config initialized:", config)
	} else {

		parts := strings.Split(clusterAddr, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid cluster address format, expected IP:PORT")
		}

		config = &src.GbConfig{
			SeedServers: []src.Seeds{
				{
					SeedIP:   parts[0],
					SeedPort: parts[1],
				},
			},
		}
		log.Println("Config initialized:", config)
	}

	// Create and start the server
	gbs := src.NewServer(name, uuid, config, nodeIp, nodePort, "8080", lc)

	go func() {
		log.Println("Starting server...")
		gbs.StartServer()
	}()

	// Block until the context is canceled
	<-ctx.Done()

	log.Println("Shutting down server...")
	gbs.Shutdown()

	return nil
}

func main() {

	fmt.Println("===================================================")
	fmt.Println("                   GoferBrokeMQ                    ")
	fmt.Println("===================================================")

	//==============================================================

	nameFlag := flag.String("name", "", "name to use")
	idFlag := flag.Int("ID", 1, "uuid for server")
	clusterAddr := flag.String("cluster", "", "address of the cluster seed")
	ipFlag := flag.String("nodeIP", "", "ip address for node")
	portFlag := flag.String("nodePort", "", "port number for node")

	flag.Parse()

	ctx := context.Background()

	// Call run and check for any errors
	if err := run(ctx, os.Stdout, *nameFlag, *idFlag, *clusterAddr, *ipFlag, *portFlag); err != nil {
		log.Fatalf("Error running server: %v\n", err)
	}

	log.Println("Server exited.")

}
