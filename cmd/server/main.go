package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/cluster"
	"log"
	"os"
)

func main() {

	fmt.Println("===================================================")
	fmt.Println("                   GoferBrokeMQ                    ")
	fmt.Println("===================================================")

	//==============================================================

	nameFlag := flag.String("name", "", "name to use")
	idFlag := flag.Int("ID", 1, "uuid for server")
	clusterIP := flag.String("clusterIP", "", "ip of the cluster seed")
	clusterPort := flag.String("clusterPort", "", "port of the cluster seed")
	clusterNetwork := flag.String("clusterNetwork", "", "network type [PUBLIC, PRIVATE, LOCAL]")
	ipFlag := flag.String("nodeIP", "", "ip address for node")
	portFlag := flag.String("nodePort", "", "port number for node")

	flag.Parse()

	fmt.Printf("Arguments: %v\n", os.Args)

	ctx := context.Background()

	// Call run and check for any errors
	if err := cluster.Run(ctx, os.Stdout, *nameFlag, *idFlag, *clusterIP, *clusterPort, *clusterNetwork, *ipFlag, *portFlag); err != nil {
		log.Fatalf("Error running server: %v\n", err)
	}

	log.Printf("Name: %s, ID: %d, ClusterIP: %s, ClusterPort: %s, ClusterNetwork: %s, NodeIP: %s, NodePort: %s\n",
		*nameFlag, *idFlag, *clusterIP, *clusterPort, *clusterNetwork, *ipFlag, *portFlag)

	log.Println("Server exited.")

}
