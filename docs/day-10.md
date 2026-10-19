# Log Store Under Partition

## Partition between the leader and a follower

The `AppendOnlyLogStore` uses a single leader replication mechanism to build a fault-tolerant durable store. The leader asynchronously communicates with its followers to replicate the log.

The following sections explain which client operations will succeed and which will experience failure if a partition separates the leader with one follower.

### Which operations will succeed?

#### Write operation on the leader

All the write operations from clients which directly hit the leader, and all the write operations from clients which hit a follower but then are routed to the leader, will succeed. Since the leader asynchronously replicates the data, it won't throw any error if a partition breaks replication on one of the follower.

#### Read operation on the unpartioned network

All the read operations from clients on any node that falls under unpartitioned network, be it the leader or any follower, will continue to work. Read on leader will provide latest updated value of a key, whereas a follower might return a stale value until it eventually get the latest value from the leader.

#### Read operation on the partitioned follower

All the read operations on the partitioned follower will still continue. However, the clients will be reading stale values since replication of latest values from the leader will not be reaching the partitioned follower.

### Which operations will fail?

#### Write on the partitioned follower

Clients that are connected to a partitioned follower will experience failures while trying to write values until partition is resolved. This happens because the follower node will not be able to forward the request to the leader.
