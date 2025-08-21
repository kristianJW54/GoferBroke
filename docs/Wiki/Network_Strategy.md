### Network Strategy

On starting a cluster - a cluster network type must be defined. If undefined, cluster network type will default to dynamic.
There are three types of network types for the cluster:
- PRIVATE
- PUBLIC
- DYNAMIC

Cluster options:

`--cluster-network-type PUBLIC`

- Public
    - Seed addresses should be public and reachable - cluster will not be valid and will not start if provided with private addresses. Cluster will not be able to dynamically become public by starting as private first

`--cluster-network-type PRIVATE`

- Private
    - Cluster will be local only and considered private and protected from global exposure. Any attempt at making public connection outside of the local environment wil be blocked

`--cluster-network-type DYNAMIC`

- Dynamic
    - Cluster can be started in either public/private. If started in private, the cluster will be considered private UNTIL a public seed node is added in which all private nodes will then attempt to form public connections with each other and public nodes using any public reachable node as a rendezvous server



When starting a cluster, a seed config with seed node addresses should be provided to bootstrap new nodes joining the cluster.
Seed node addresses must be public and reachable (able to act as a rendezvous server) in order for the cluster to be global/public.
If the seed addresses are private/on localhost or behind a private NAT, the cluster will be flagged as local (dev mode, test mode etc).

---

Once a cluster is started with a defined network type - the seed addresses will attempt to resolve.
If there is no seed addresses specified, a default localhost address and port will be provided and a warning will be shown that
the cluster is on PRIVATE and will only accept private node connections. Nodes will then compare themselves to the default seed address to determine if they are the seed.
If they are not the seed, a node will attempt a connection to the seed. If that attempt fails because there is no seed to connect to, then a retry phase will begin
and the node will backoff and fail if no seed joins.

**For production use - Seed Addresses should ALWAYS be provided and Network type specified**

**Seeds must be started first**

When a seed is successfully started and the initial network type is defined, the seed node will determine its actual network type and its reachability.
This is based on a trust system whereby if the user has specified the network type then we accept that so long as it does NOT conflict with the address provided and initial network check
Example:
- NetworkType defined as PUBLIC - Network IP check = LOOPBACK_ONLY --> Error mismatch
- NetworkType defined as PUBLIC - Network IP check = PublicUndefined --> PublicReachable (we trust the user specification and will upgrade based on inbound connection - no STUN check)

Once the initial checks are made - the seed node and cluster can accept node connections.

---

Node Start:

    -nodeAddr="156.XXX.XXX:8000" -nodeNetworkType=PUBLIC
    -nodeAddr="210.XXX.XXX:8000" -nodeNetworkType=PRIVATE

On starting a new node, the node will determine it's initial Network Type based on the IP provided and the network type if specified
- Loopback
- Private
- Unreachable
- PublicUndefined

If NetworkType Specified:
- If the node has a specified network type, we 'trust' the specified type.
    - If PUBLIC - we will trust as public reachable AFTER the network check confirms PublicUndefined. This will be upgraded to PublicOpen AFTER successful inbound connections made
    - If PRIVATE - we will trust as private and determine what level of private, if loopback etc.
    - If Unspecified - we will perform full network discovery through the state machine until we are able to define the network type

STUN:
- If the Node is Private it will perform STUN to discover what it's public facing address is.
    - If the IP and Port match the external IP and Port then we are hole-punchable and/or public reachable
    - If Port is different - then NAT is re-writing the Port meaning we may be hole-punchable
    - Else we mark as Unreachable and it remains Private

Once the node has joined, and its network type has been determined the node can then work to refine and upgrade its network type based on the cluster network type.

Cluster Network is Dynamic:

If the cluster network is dynamic, when a new node joins with a private network type, the node will do a STUN check to further refine its type.
If it discovers that it can hole-punch using a rendezvous it will then use a PublicReachable node to do this (preferably one of the seed nodes)

If it is found that the node is not reachable or is on a loopback or private address which cannot be hole-punched, then the node will be rejected from the cluster

A cluster cannot start as PRIVATE and be upgraded to PUBLIC. Initial seed nodes must be PublicReachable or the cluster will be PRIVATE Only.
Nodes who join and are PRIVATE, can then attempt to be made PUBLIC through NAT Traversal, if this fails then they are rejected.

Cluster Network is Public:

All nodes which are private will be rejected - no hole-punching or STUN checks

Cluster Network is Private:

All node must be private, any public node will be rejected