<p align="center">
  <img src="https://github.com/kristianJW54/GoferBroke/blob/main/GoferBroke%20(3)%20-%20Copy.png" alt="GoferBroke Logo" width="250" style="vertical-align: middle; margin-right: 10px;"/>
</p>

# GoferBroke

**A Fast, Lightweight Anti-Entropy Gossip Protocol for Distributed Systems**

> *Turn any app into a distributed system.*

<p align="center">
  <a href="https://goreportcard.com/report/github.com/kristianJW54/GoferBroke"><img src="https://goreportcard.com/badge/github.com/kristianJW54/GoferBroke" alt="Go Report Card" /></a>
  <a href="https://pkg.go.dev/github.com/kristianJW54/GoferBroke"><img src="https://pkg.go.dev/badge/github.com/kristianJW54/GoferBroke.svg" alt="Go Reference" /></a>
  <a href="https://github.com/kristianJW54/GoferBroke/blob/main/LICENSE"><img src="https://img.shields.io/github/license/kristianJW54/GoferBroke.svg?style=flat-square" alt="License" /></a>
</p>

## Overview

GoferBroke is a minimal, high-performance gossip protocol designed for embedding decentralized, eventually-consistent state into your application instances. Built on TCP with custom framing, it allows every node to gossip deltas (state changes) and reconcile with others through a compact, version-aware protocol.

**No brokers. No databases. No event streams. Just peers sharing state.**

## Key Concept

> **Every application instance becomes a node in a decentralized system.**

Each node:

- Gossips its state as `deltas`
- Reconciles state from peers using version-aware merges
- Tracks other nodes' presence and health
- Triggers events that applications can subscribe to (`onEvent`)
- Detects Failures both direct and indirectly
- Distributes Cluster Configuration and Applies State Changes

---

## Features

- **Efficient delta-based reconciliation**
- **Custom binary protocol over TCP** (supports partial packets)
- **Decentralized cluster** with seed-based bootstrapping
- **User-defined deltas** for complete flexibility
- **Built-in failure detection**
- **Hookable event system** for specifiying custom logic on events

---

## Real-World Use Cases

### 1. **Live Cluster Awareness for Stateless Services**
Auto-discover and track peer nodes, readiness, zones, and versions.

### 2. **Feature Flag Propagation**
Distribute toggles like `feature_x_enabled = true` without needing Redis/Kafka.

### 3. **IoT / Edge Mesh Coordination**
Field devices self-organize and exchange state without central authority.

### 4. **In-Process Cluster Membership**
Gossip directly from your app logic — no sidecars or agents.

### 5. **Lightweight Control Bus**
Propagate live config like `maxConnections: 1000` across the cluster.

### 6. **Game or Simulation State Sync**
Sync object/player/entity data in real-time between peers.

---

## Why Use GoferBroke?

- You want **decentralized state** without infra.
- You want to **embed a fast, robust sync layer** into your Go app.
- You need **live coordination without brokers**.
- You want **customizable deltas and hooks** to drive logic.

> *GoferBroke turns any app into a distributed system.*

## Roadmap

- Custom TCP Protocol ✅

- Nodes Joining and Leaving Cluster ✅

- Gossip Framework ✅

- Delta and Digest Parsing + Serialising ✅

- Gossip Rounds Working and Exchanging ✅

- Failure Detection ✅

- Cloud Platform Testing ✅

- Go SDK ✅

- Custom Configuration Parsing ✅

- Async Logging and Log Buffers ✅

- Add Memory Management Delta Updates and Events

- Build out Client Commands and Connection Handling

- Implement full discovery phase for joining nodes of a big cluster

- mTLS Encryption

- Flow Control For High Load Gossip

- NAT Hole-Punching and NAT Traversal

- Client SDK and CLI Tool

## Getting Started

### Examples

> Running multiple nodes on a single machine may cause variance in gossip round durations and possible deadline timeouts being reached.
> I highly suggest running on multiple computers using different ports on private IPs for same network testing

You can find working examples in the [Examples Folder](https://github.com/kristianJW54/GoferBroke/tree/main/Examples/basic_node).

For example, try running a basic `node` to quickly start a local cluster.

1. Start by declaring our configuration.

GoferBroke has a custom lexer/parser if wanting to load from file, or we can declare config structs and pass those in instead.

```go
// Declare cluster wide config - same for all application instances - changes would be gossiped to nodes and applied
clusterConfig := &gossip.ClusterConfig{
    Name: "database_cluster",
    SeedServers: []gossip.Seeds{
        {
            SeedHost: "localhost",
            SeedPort: "8081",
        },
    },
    NetworkType: "LOCAL",
}

node1Config := &gossip.NodeConfig{
    Name:        "node-1", // -- Pulled from envar
    Host:        "localhost", // -- Pulled from envar
    Port:        "8081", // -- Pulled from envar
    NetworkType: "LOCAL", // -- LOCAL as we are using localhost loopback only
    IsSeed:      true, // If seed then the address would also need to be in cluster config
    ClientPort:  "8083", // -- What port you want clients (non-nodes) to connect to
}

```

2. Add another node

Adding another `node` should ideally be done in another application instance.

In your application you would want to dynamically load new `node` configs for each instance and include the same cluster config.

But for this example we will launch both nodes on the same machine.

```go
node2Config := &gossip.NodeConfig{
    Name:        "node-2",
    Host:        "localhost",
    Port:        "8082", // -- different port to node-1 (pulled from envar ideally)
    NetworkType: "LOCAL",
    IsSeed:      false, // -- We are not a seed and so shouldn't have our address in cluster config
    ClientPort:  "8083",
}
```


3. Start gossiping - and that's it!

```go
node1, err := gossip.NewNodeFromConfig(clusterConfig, node1Config)
if err != nil {
    panic(err)
}

node2, err := gossip.NewNodeFromConfig(clusterConfig, node2Config)
if err != nil {
    panic(err)
}

node1.Start()
node2.Start()

```

### Run Locally Through CLI

Clone the project

```bash
  git clone https://github.com/kristianJW54/GoferBroke
```

Start a seed node in terminal

```bash
  go run ./cmd/server -mode=seed -name=test1 -clusterNetwork=LOCAL -nodeAddr="localhost:8081" -routes="localhost:8081"
```

Start a second node in another terminal

```bash
  go run ./cmd/server -mode=node -name=test2 -clusterNetwork=LOCAL -nodeAddr="localhost:8082" -routes="localhost:8081"
```

To run two non-local nodes

note: clusterNetwork must be set to the network type of the IP being used e.g.
- all local node = LOCAL
- all private nodes = PRIVATE
- all public nodes = PUBLIC,
- a mixture of public and private nodes = DYNAMIC

refer to Network_Strategy.md for further info)

```bash
go run ./cmd/server -mode=seed -name=test1 -clusterNetwork=PRIVATE -nodeAddr="192.168.1.xxx:5000" -routes="192.168.1.xxx:5000"
  go run ./cmd/server -mode=node -name=test2 -clusterNetwork=PRIVATE -nodeAddr="192.168.1.xxx:8082" -routes="192.168.1.xxx:5000"
```

## Contributing

Contributions are welcome and appreciated. Whether you're fixing a bug, adding a feature, or improving documentation, your efforts help make this project better for everyone.

**How to Contribute**

- Fork the Repository: Start by forking the repo and cloning it to your local machine.

- Create a Branch: Create a feature or bugfix branch to work on your changes.

- Make Changes: Write clear, well-structured code with comments where necessary. Ensure any new features are appropriately documented and tested.

- Submit a Pull Request: Open a pull request (PR) with a clear description of the changes and the problem being solved.

**Areas for Contribution**

As the project is in early development, the following areas need special attention:

- Source Code: Enhance functionality, fix bugs, or add new features to the library.

- Optimizations: Improve performance, reduce overhead, and enhance scalability.

- Test Writing: Expand the test suite to ensure robustness and prevent regressions.

- Documentation: Add or improve examples, tutorials, and API documentation.

- Error Handling: Help improve error reporting with clear, actionable error messages.
Guidelines
---

Follow the existing coding style and conventions.
Write unit tests for all new functionality and ensure all tests pass before submitting.
Keep pull requests focused; avoid bundling unrelated changes.
Ensure changes are backward-compatible where possible.

Get in Touch
If you’re unsure where to start or have questions, feel free to open a discussion or create an issue. I'm happy to help :)

## Papers & Foundations

### [Efficient Reconciliation and Flow Control for Anti-Entropy Protocols (FlowGossip)](https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf)
A foundational paper by van Renesse et al. describing anti-entropy protocols, reconciliation strategies, and flow control for scalable, fault-tolerant state replication.

Your contributions are what make this project thrive—thank you for supporting GoferBroke!

