# Phase 01

## Log store

### About the store

The [AppendOnlyLogStore](../log-store/src/main/java/lab/logstore/AppendOnlyLogStore.java) is a durable key-value store. In its present form it:

- writes are always sequential i.e. new inserts will always be appended
- read operations are not sequential

### Rejects torn entry

The store reliably starts after crash by rejecting the torn entry, and thus allows the clients to lookup remaining keys.

If the torn entry had an updated value of an existing key then the store records the last complete value as the latest value of the key.

### Key indexes

The store builds an index on load by mapping every key to its corresponding offset in the file from where its value can be read.

Any new write to the store, after building the intial index, will update the index inline.

### Latency target

The store must respond to read calls, at 100 requests per second, within 200 milliseconds.
