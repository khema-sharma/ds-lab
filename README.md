# DS Lab

## Problem

This repository is a lab for durable key-value store. The first module is `log-store`. It has two append-only stores [FileLogStore](./log-store/src/main/java/lab/logstore/FileLogStore.java) and [InMemoryLogStore](./log-store/src/main/java/lab/logstore/InMemoryLogStore.java).

## Design

The parent build uses Java21. The `log-store` contains `Property` and `PropertyDefinition`. A `PropertyDefinition` cannot be constructed with a blank metric, unit, target, or failure. The three values are in [Glossary](./docs/glossary.md).

## Failure modes

The tests reject a blank metric, a blank unit, a blank target, and a blank failure. The module does not recover from a torn write, and it yet does not serve a get.

## How to run the tests

From the repository root, run `./mvnw -B test` command. The GitHub workflow [Daily Build](./.github/workflows/ci.yaml) uses the same command on every push.

## What would change at 10 times the load

Load test with one caller at a time exists. It does not tell us what happens when 1,000 gets arrive each second. At that rate we would need a new measurement of the 99th percentile time, because the definition stored in memory has no clock and cannot answer the question.

## Day 2 measurements

Load testing includes running 100000 reads and writes on a map based store and a file based store and computing their median and 99th percentile time values.

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
