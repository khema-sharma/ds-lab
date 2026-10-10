# DS Lab

## Problem

This repository is a lab for durable key-value store. The first module is `log-store`. Today it only defines three properties and a Lamport clock.

## Design

The parent build uses Java21.

### log-store module

The `log-store` contains:

- `Property` and `PropertyDefinition`. A `PropertyDefinition` cannot be constructed with a blank metric, unit, target, or failure. The three values are in [Glossary](./docs/glossary.md).

- Two stores [InMemoryStore](./log-store/src/main/java/lab/logstore/InMemoryStore.java), which is a hash map based memory store, and [AppendOnlyLogStore](./log-store/src/main/java/lab/logstore/AppendOnlyLogStore.java) which is a file based append only log store.

- [LamportClock](./log-store/src/main/java/lab/logstore/LamportClock.java) which ensured ordered events and [DistributedProcess](./log-store/src/main/java/lab/logstore/DistributedProcess.java) which mimics a process running on a distributed node.

## Failure modes

The tests reject a blank metric, a blank unit, a blank target, and a blank failure. The module does not recover from a torn write.

The [Lamport clock tests](./log-store/src/test/java/lab/logstore/LamportTest.java) accepts ordered events on distributed processes when the processes communicate with each other and accept concurrent events when they happen without any communication between processes.

## How to run the tests

From the repository root, run `./mvnw -B test` command. The GitHub workflow [Daily Build](./.github/workflows/ci.yaml) uses the same command on every push.

## What would change at 10 times the load

Load test with one caller at a time exists. It does not tell us what happens when 1,000 gets arrive each second. At that rate we would need a new measurement of the 99th percentile time, because the definition stored in memory has no clock and cannot answer the question.

## Day 2 measurements

Load testing includes running 100,000 reads and writes on a map based store and a file based store and computing their median and 99th percentile time values.

The below table summarizes the load test run on day-02.

| Path | Median | 99th percentile |
| ---- | ------ | --------------- |
| Map put | 0.000083 ms | 0.000208 ms |
| Map get | 0.000083 ms | 0.000250 ms |
| File write | 0.001792 ms | 0.003250 ms |
| File read | 1.915208 ms | 3.632250 ms |

>Note:
>
>Each file read starts at the first line and reads forward until it finds the requested key. A key written near the end of the file therefore takes longer than a key written near the start. The 99th percentile is the time for one of those late keys, not the time for a read that can jump directly to the record.
