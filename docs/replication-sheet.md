# Replication

A single node log-store cannot be fault-tolerant. To make it fault-tolerant multiple instances of the store must be running so that if one node goes down then the others can share the load, until the failed node gets back. This makes the storage service available even when faults occur.

Data redundency is the primary way to achieve this. Data is replicated on multiple nodes so that the storage service can continue serving with any of the available node.

The `log-store` module supports following replication modes:

- Asynchronous single-leader replication, and
- Synchronous, leaderless quorum based replication

## Asynchronous single-leader replication

To set up an asynchronous single-leader replication, one needs to create multiple instances of `AppendOnlyLogStore` class. Each instance represent a node in a distributed system. Amongst the instances, one must be a leader and the rest must be followers. The leader will asynchronously replicate the data onto the follower nodes whenever data is written to the store.

### Protection by fencing

The `AppendOnlyLogStore` uses `LamportClock` to generate fecing term and is designed to avoid split-brain. The store first checks if its fecing term is greater than the latest term used in the system. If not, it rejects a write operation.

### Selecting a leader

#### Via configuration

Example:

```java
try (AppendOnlyLogStore leader = new AppendOnlyLogStore(LogStoreRole.LEADER)) {
    try (AppendOnlyLogStore followerA = new AppendOnlyLogStore(LogStoreRole.FOLLOWER) {
        try (AppendOnlyLogStore followerB = new AppendOnlyLogStore(LogStoreRole.FOLLOWER) {
            leader.registerFollower(followerA);
            leader.registerFollower(followerB);

            leader.put("key-1", "value-1");

            // Could print `null` or `value-1`
            System.out.println(followerA.get("key-1"));
        }
    }
}
```

#### Via election

The store, as of today, does not have an automatic leader selection process (using consensus) in place. To fake an election process, the `AppendOnlyLogStore` provides `becomeLeader` method which makes the enclosing store object a leader.

Example:

```java
try (AppendOnlyLogStore nodeA = new AppendOnlyLogStore()) {
    try (AppendOnlyLogStore nodeB = new AppendOnlyLogStore() {
        try (AppendOnlyLogStore nodeC = new AppendOnlyLogStore() {
            nodeA.becomeLeader();

            nodeA.registerFollower(nodeB);
            nodeA.registerFollower(nodeC);

            nodeA.put("key-1", "value-1");

            // nodeB is the new elected leader
            nodeB.becomeLeader();
            nodeB.registerFollower(followerC);
            
            // nodeB node replicates the entry to a new leader
            nodeB.put("key-2", "value-2");

            // old leader rejects this write by throwing IllegalStateException
            nodeA.put("key-3", "value-3");
        }
    }
}
```

### Failure modes

As of this phase, the single-leader replication has following limitations:

- Data is replicated asynchronously and thus any failure in replication results in a lost replication
- Niether the leader, nor the followers have mechanism in place which them to get synchronized again in case of a node failure
- If leader goes down then there is no automatic new leader election process, thus the entire system goes down

## Leaderless quorum based replication

In this replication mode, the task of duplicating the data is taken up by an external client. The stores do not participate in any election process. They act as standalone stores. The `AppendOnlyLogStoreClient` is the class which allows users to set up quorum based replication. The following code snippet shows it how.

Unlike the single-leader based replication, here the external client ensures that every put operation performed through it succeeds only when it can successfully perform put operation on all quorum nodes. Similarly a get operation is successful only when the quorum read completes.

```java
// Creates a quorum of three nodes
try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(3)) {
    // Completes when all the nodes in the quorum write the value,
    // otherwise throws an exception
    storeClient.put("key-1", "value-1");

    // Completes when all the nodes in the quorum read the value,
    // otherwise throws an exception
    String value = storeClient.get("key-1");
}
```

### Quorum failure modes

Like in single-leader based replication there is no automatic way of dealing with node failures and there is no mechanism in place to detect whether a node is down.
