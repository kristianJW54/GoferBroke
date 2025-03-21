package main

import (
	"GoferBroke/internal/Cluster"
	"context"
	"flag"
	"fmt"
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
	ipFlag := flag.String("nodeIP", "", "ip address for node")
	portFlag := flag.String("nodePort", "", "port number for node")

	flag.Parse()

	fmt.Printf("Arguments: %v\n", os.Args)

	ctx := context.Background()

	// Call run and check for any errors
	if err := Cluster.Run(ctx, os.Stdout, *nameFlag, *idFlag, *clusterIP, *clusterPort, *ipFlag, *portFlag); err != nil {
		log.Fatalf("Error running server: %v\n", err)
	}

	log.Printf("Name: %s, ID: %d, ClusterIP: %s, ClusterPort: %s, NodeIP: %s, NodePort: %s\n",
		*nameFlag, *idFlag, *clusterIP, *clusterPort, *ipFlag, *portFlag)

	log.Println("Server exited.")

}
