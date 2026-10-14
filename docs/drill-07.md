# Day-07 drill

## The API

The `AppendOnlyLogStore` is an durable append-only key-value store backed by filesystem. Clients can write key-value pairs and read them back using following APIs:

| API | Description |
| --- | ----------- |
| `void write(String key, String value) throws IOException` | Write given key-value pair to the store. It throws an `IOException` if store fails to write the key-value pair. |
| `String read(String key) throws IOException` | Returns the value of the given key. If key is not present then it returns `null`.  It throws an `IOException` if store fails to read the key-value pair.|

## Crash recovery

Every key-value pair (a.k.a data) that is inserted in the store will also store additional metadata about the pair in order to recover from crash. Key-value pair along with the metatdata is referred to as store entry.

Each store entry holds the length of the data and its checksum in it. Length and checksum are used to determine if an entry in the store is corrupted due to crash in the middle of a write.

If store dies while writing an entry then the unfinished entry stays in the file which backs the store. On the next load, the store keeps all complete records and rejects the last torn record.

## Latency target

The 99th percentile time of a `read` operation, at 100 requests per second, must stay within 200 milliseconds.

## What is not built?

The `AppendOnlyLogStore` is a single node store without any replication.
