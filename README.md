# DS Lab

## Problem

This repository is a lab for durable key-value store. The store is an append-only file. The log is the source of truth. The store adopts Single Leader Replication strategy to maintain copies of its data. It does not have a second log and it does not use a quorum to commit reads or writes.

## Design

### Record

A record in the store is a length, a checksum, and a key-value pair. The store rejects a broken record at the tail. Earlier records stay readable. The store also maintains indexes of records. The index maps key to the offset of the latest complete record in the file. It is rebuilt on open and updated on write operation.

### Roles and acknowledgement

A store can act as a leader or as a follower. Leader store appends the write operation and replicates the record to its followers. Replication is asynchronous, so write returns after the leader has appended the record.

### Term

Leader store gets a replication term on its construction, or when a follower store is made a leader, from a central replication state. Replication term only increments. Store, be it a follower or a leader, rejects a write if its current term is lesser than the current replication term in use (to avoid split-brains).

### Partition

The `partitionFollower` method marks one follower as partitioned from the leader. The leader skips replication on partitioned follower. The partitioned follower rejects (throws exception) any writes. Reads on it still return the last value it has.

### Property and property definition

`Property` is the name: scalability, reliability, or maintainability. A `PropertyDefinition` cannot be constructed with a blank metric, unit, target, or failure. The three values are in [Glossary](./docs/glossary.md)

### In-memory store

In-memory store uses a map to store key-value pair. It is guarded for one writer and multiple readers by a single monitor lock.

## Failure modes

The store handles following failure modes:

- It rejects torn write at the tail
- It accepts last complete value of a write
- It rejects write from an old leader (failover)
- It skips partitioned follower
  - Partitioned follower rejects write
- It rejects duplicate write with same idempotency key

## Measurements

Load testing includes running 100,000 reads and writes on a map based store and a file based store and computing their median and 99th percentile time values.

### Day 2 numbers

The below table summarizes the load test run on day-02.

| Path | Median | 99th percentile |
| ---- | ------ | --------------- |
| Map put | 0.000083 ms | 0.000208 ms |
| Map get | 0.000083 ms | 0.000250 ms |
| File write | 0.001792 ms | 0.003250 ms |
| File read | 1.915208 ms | 3.632250 ms |

## Load

The 99th percentile time of the store's read operation, at 100 requests per second, must be within 200 milliseconds. The current benchmark does not measure that.

## How to run the tests

From the repository root, run `./mvnw -B test` command. The GitHub workflow [Daily Build](./.github/workflows/ci.yaml) uses the same command on every push.
