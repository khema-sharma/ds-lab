# Phase 02

## Log store update

### About the store

The `log-store` module provides a durable key-value store: [AppendOnlyLogStore](../log-store/src/main/java/lab/logstore/AppendOnlyLogStore.java). It is an append-only store which always appends data to a durable file. It builds an in-memory index for faster retrieval.

### Election and term

A single node store is not fault-tolerant. To make a store fault-tolerant, one must be able to set up replications, and as well be able to partition the data-set between multiple stores.

Only a distributed log store can provide availability and durability guaranteed. Consensus forms the building block of a distributed system. The `log-store` module still lacks this.

A production grade consensus system requires majority voting and timeout mechanisms to be in place.

### Manual election and fecing term

The module, however, does allow users to setup a playground, with multiple nodes, which can mimic a distributed system. The `AppendOnlyLogStore` provides a constructor which takes the [LogStoreRole](../log-store/src/main/java/lab/logstore/LogStoreRole.java) argument which instructs the store to play the given role.

To create a simple three node single-leader replication system, do the following:

- Create three instances of the `AppendOnlyLogStore` class
  - For the first instance, pass `LogStoreRole.LEADER` as its role in the constructor
  - For the other two instances, pass `LogStoreRole.FOLLOWER` as the role
- Add second and third nodes as followers of the leader node by calling `AppendOnlyLogStore.registerFollower` method

The store checks fencing token (term) before performing a write operation. Thus, only the leader will be able to perfrom write where as the other nodes will reject a write operation. A test case backs this claim.

To mimic leader failover, call `AppendOnlyStore.becomeLeader` method on any of the follower node to make it the new leader. Register the other follower node as its (new leader's) follower. This automatically makes the new leader to get a higher fencing term than the old leader. Thus only the new leader will be able to perform the write operation. The old leader will start rejecting new write requests.

### Coordination service

Usually consensus systems are built using coordination service. A coordination service plays the role of a coordinator when it comes to electing a leader, detecting failovers (to avoid split-brains) etc. It is this service which provides an implementation of consensus algorithm(s) which can be used by the other nodes to reach to a consensus.

#### Building vs buying

The algorithms provided by the coordination services are often quite complex to build. If a team requires fault-tolerant consensus system to be in place, i.e. a system which can tolerate network delays/partitions, then it is often better to purchase a coordination service instead of building one from scratch.
