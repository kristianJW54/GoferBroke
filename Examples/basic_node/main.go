package basic_node

// Basic Node using user specified config structs

// Example:

// A simple database application wants to distribute it's state and become highly available and fault-tolerant
// To do so it needs a protocol for replication such as Paxos or Raft but also a lightweight p2p gossip protocol
// to ensure availability and failure detection

// On each instance of the application a new gossip node will be created alongside the database instance
// let's call it node-1

// application-1 will be created and become the leader of a new database cluster and embedded with it will be node-1

// It will be created with a cluster wide config that the whole of the cluster will adhere and, its own internal node config
// which marks it as a unique node the cluster

// Example code block
//
//
//
//
//
//
//
//
//
//
//
//
// Example code

// We now need to distribute the database and expand the cluster. To do so we create a new application instance with the same
// Cluster config, but this time we dynamically create a new node config which will specify a new node

// this will be called node-2

// Application-2 will join the cluster as a follower (as per Raft protocol) but crucially for the gossip protocol, node-2 will be embedded and will reach out
// to node-1 and the two will begin gossiping state and exchanging deltas

// We now have a gossip cluster and the database application has two distributed instances which are keeping in sync and detecting each others
// failures or state changes

// the application can now continue with its purpose of replicating database logs from leaders to followers with a high degree of certainty that
// nodes in the cluster are available
