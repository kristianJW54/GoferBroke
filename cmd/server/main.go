package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"log"
	"os"
	"strings"
)

type clusterSeedAddrs []string

func (a *clusterSeedAddrs) String() string {
	return strings.Join(*a, ",")
}

func (a *clusterSeedAddrs) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func main() {

	// Run command = ./cmd/server ....

	var routes clusterSeedAddrs

	modeFlag := flag.String("mode", "", "Mode to run: seed | node")
	nameFlag := flag.String("name", "", "Name of node")
	addrFlag := flag.String("nodeAddr", "", "Node address in host:port format")
	clusterNetwork := flag.String("clusterNetwork", "LOCAL", "Network type [PUBLIC, PRIVATE, LOCAL]")
	nodeFileConfig := flag.String("nodeConfig", "", "Configuration file for this node")
	clusterFileConfig := flag.String("clusterConfig", "", "Configuration file for this cluster")
	clientPort := flag.String("clientPort", "", "client port to listen on")
	profilingPort := flag.String("profilingPort", "", "Profiling port")

	flag.Var(&routes, "routes", "Route addresses - can be specified multiple times")

	flag.Parse()

	// Validate common fields
	if *modeFlag != "seed" && *modeFlag != "node" {
		log.Fatalf("Invalid --mode specified: must be 'seed' or 'node'")
	}
	if *addrFlag == "" {
		log.Fatalf("--nodeAddr is required")
	}
	if *nameFlag == "" {
		log.Fatalf("--name is required")
	}
	if len(routes) == 0 && *clusterFileConfig == "" {
		log.Fatalf("--routes are required when no --clusterConfig is specified")
	}

	fmt.Printf("Arguments: %v\n", os.Args)

	ctx := context.Background()

	// Run cluster logic
	if err := cluster.Run(
		ctx,
		os.Stderr,
		*modeFlag,
		*nameFlag,
		routes, // use as []string directly
		*clusterNetwork,
		*addrFlag,
		*clientPort,
		*nodeFileConfig,
		*clusterFileConfig,
		*profilingPort,
	); err != nil {
		log.Fatalf("Error running server: %v\n", err)
	}

	fmt.Printf("[BOOT] Mode: %s, Name: %s, Addr: %s\n", *modeFlag, *nameFlag, *addrFlag)
}
