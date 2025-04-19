<p align="center">
  <img src="https://github.com/kristianJW54/GoferBroke/blob/main/GoferBroke%20(3)%20-%20Copy.png" alt="GoferBroke Logo" width="250" style="vertical-align: middle; margin-right: 10px;"/>
</p>

# GoferBroke

**An Anti-Entropy P2P Gossip Protocol for Distributed Systems**

Work in progress!
## Description

GoferBroke is a lightweight, extensible library designed for building distributed clusters using an anti-entropy gossip protocol over custom binary TCP. It provides a robust foundation for ensuring efficient data synchronization, eventual consistency, and scalability across a wide range of environments, including cloud, on-premises, and hybrid systems.

With GoferBroke, users can deploy a highly configurable cluster of nodes to distribute application state or data reliably and efficiently. Its fault-tolerant design ensures high availability even in the presence of network partitions or node failures. The library supports custom configurations for networking, discovery, security, and gossip parameters, making it adaptable to various use cases, such as state replication, file distribution, and load balancing.

Built for developers and system architects, GoferBroke simplifies the complexities of distributed system design, providing powerful tools and a flexible framework for building scalable, resilient cluster-based solutions over TCP.
## Features

- Anti-Entropy Gossip Protocol: Ensures eventual consistency and efficient data synchronization by exchanging deltas with version tracking.

- Custom Binary TCP Protocol: Uses a lightweight, binary communication layer for high performance and low overhead.

- Highly Configurable: Flexible options for networking, gossip intervals, security settings and scaling.

- Fault Tolerance: GoferBroke implements Phi Accrual failure detection to ensure node failures are detected whilst ensuring gossip is propagated through the cluster.

- Cluster Scalability: Easily scale clusters to handle diverse workloads across cloud, on-premises, and hybrid environments.

- Pluggable Architecture: Extend functionality to support use cases like shard assignment, file transfer, caching etc. Through the use of custom deltas which are gossiped and Handlers which allows users to define application logic.


## Roadmap

- Custom TCP Protocol ✅

- Nodes Joining and Leaving Cluster ✅

- Gossip Framework ✅

- Delta and Digest Parsing + Serialising ✅

- Gossip Rounds Working and Exchanging ✅

- Phi Accrual Failure Detection ✅

- Cloud Platform Testing ✅

- Go SDK

- Custom Configuration Parsing

- CLI Tool

- Network Types and NAT Handling

- Internal Monitoring + Tracing

- mTLS Encryption

- Flow Control For High Load Gossip


## Run Locally

Clone the project

```bash
  git clone https://github.com/kristianJW54/GoferBroke
```

Start a seed node in terminal

```bash
  go run ./cmd/server -name=test -ID=1 -clusterNetwork=LOCAL -nodeIP=localhost -nodePort=8081
```

Start a second node in another terminal

```bash
  go run ./cmd/server -name=test -ID=2 -clusterNetwork=LOCAL -nodeIP=localhost -nodePort=8082
```

To run on another machine (note: clusterNetwork must be set to the network type of the IP being used e.g. local->local=LOCAL, private->private=PRIVATE, public->public=PUBLIC, public & private=DYNAMIC
- refer to Network_Strategy.md for further info)

```bash
go run ./cmd/server -name=test1 -ID=1 -clusterIP="192.168.1.xxx" -clusterPort=8081 -clusterNetwork=PRIVATE -nodeIP="192.168.1.xxx" -nodePort=8081
  go run ./cmd/server -name=test1 -ID=2 -clusterIP="192.168.1.xxx" -clusterPort=8081 -clusterNetwork=PRIVATE -nodeIP="192.168.1.xxx" -nodePort=8082
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
If you’re unsure where to start or have questions, feel free to open a discussion or create an issue. I'm appy to help :)

Your contributions are what make this project thrive—thank you for supporting GoferBroke!

