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

GoferBroke is a minimal, high-performance gossip protocol designed for embedding decentralized, eventually-consistent state into your application instances. Built on TCP with custom framing and no external dependencies, it allows every node to gossip deltas (state changes) and reconcile with others through a compact, version-aware protocol.

**No brokers. No databases. No event streams. Just peers sharing state.**

## Key Concept

> **Every application instance becomes a node in a decentralized system.**

Each node:

- Gossips its state as deltas
- Reconciles state from peers using version-aware merges
- Tracks other nodes' presence and health
- Triggers events that applications can subscribe to (`onEvent`)
- Remains resilient in partitions and churn

---

## Features

- **Efficient delta-based reconciliation**
- **Custom binary protocol over TCP** (supports partial packets)
- **Decentralized cluster** with seed-based bootstrapping
- **No external dependencies** — runs as a CLI or embeds in any Go app
- **User-defined deltas** for complete flexibility
- **Flow control with overload logging**
- **Built-in phi-accrual failure detection**
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
- You want to **stay simple and dependency-free at runtime**.

> *GoferBroke turns any app into a distributed system.*

## Roadmap

- Custom TCP Protocol ✅

- Nodes Joining and Leaving Cluster ✅

- Gossip Framework ✅

- Delta and Digest Parsing + Serialising ✅

- Gossip Rounds Working and Exchanging ✅

- Phi Accrual Failure Detection ✅

- Cloud Platform Testing ✅

- Go SDK ✅

- Custom Configuration Parsing ✅

- CLI Tool

- Network Types and NAT Handling

- Internal Monitoring + Tracing

- mTLS Encryption

- Flow Control For High Load Gossip

## Getting Started

### Examples

You can find working examples in the [Examples Folder](https://github.com/kristianJW54/GoferBroke/tree/main/Examples/basic_node).

For example, try running a basic node to quickly start a local cluster.


### Run Locally

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

### [The Phi Accrual Failure Detector](https://www.researchgate.net/publication/29682135)
Presents a flexible, statistically grounded failure detector that outputs suspicion levels over time instead of boolean states. Ideal for adaptive fault tolerance in distributed systems.

Your contributions are what make this project thrive—thank you for supporting GoferBroke!

