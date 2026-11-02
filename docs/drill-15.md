# Multi-region key-value store

## Design

A multi-region key-value store is a distributed system designed for high availability, and with fault-tolerance mechanism in place, with nodes spread accross multiple regions.

There are multiple ways to design this depending on the trade-offs we want to make:

1. Single-leader based setup
2. Multi-leader based setup and
3. Leaderless setup

### Single-leader based setup

This setup can support linearized write and read operations which can guarantee happens-before and causality between two writes and between writes and reads. A coordination service can be used to implement automatic leader election process and to get votes before each write.

Here availability and latency are traded off for consistency.

When the nodes, spread accross regions, boot up they start election process to choose one of the node as the leader. Once a leader is chosen, all other nodes work as a follower.

Only the leader node can perform write operation. If a write operation reaches follower node then the followe node will redirect the request to the leader node.

#### Zone failure in a single-leader setup

If a zone is cut off then:

- If the zone had the leader, then all write operations will start failing and there will be down time until another leader gets elected in some other zone
- The followers present in other zones could be behind the leader by some commits and hence the reads landing on those followers will return stale values until a new leader with up to date data comes back

## Multi-leader setup

Here multiple leaders can be setup, one in each region. Multi-leader systems cannot be linearized.

Here consistency is traded-off for high availability and latency.

### Zone failure in a multi-leader setup

If a zone is cut off then:

- Other zones continue to work without any reconsiliation with the cut-off zone
- This could cause data integrity issues if data in multiple zones work as a whole