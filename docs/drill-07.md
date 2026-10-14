# Day-07 drill

## The API

The `AppendOnlyLogStore` is an append-only key-value store. Clients can write key-value pairs and read them back using following APIs:

| API | Description |
| --- | ----------- |
| `write(String key, String value)` | Write given key-value pair to the store |
| `read(String key)` | Reads the value of the given key |

## Crash recovery

Every key-value pair (a.k.a data) that is inserted in the store will also store additional metadata about the pair in order to recover from crash. Key-value pair along with the metatdata is referred to as store entry.

Each store entry holds the length of the data and its checksum in it. Length and checksum are used to determine if an entry in the store is corrupted due to crash in the middle of a write.

## Latency target

The 99th percentile time of a `read` operation, at 100 requests per second, must stay within 200 milliseconds.

## What is not built?

The `AppendOnlyLogStore` is a single node store without any replication.
