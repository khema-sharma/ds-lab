package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

public class LogStoreTest {

    @Test
    public void acceptsValidWrites() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-2", "value-2" } });

            assertEquals("value-1", store.get("key-1"));
            assertEquals("value-2", store.get("key-2"));
        }
    }

    @Test
    public void acceptsWritesBetweenRestarts() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();
            insertRecords(store, new String[][] { { "key-1", "value-1" } });
        }

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            insertRecords(store, new String[][] { { "key-2", "value-2" } });

            assertEquals("value-1", store.get("key-1"));
            assertEquals("value-2", store.get("key-2"));
        }
    }

    @Test
    public void rejectsTornWrite() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            insertRecords(store, new String[][] { { "key-1", "value-1" } });
            tearTailRecord(store);
        }

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals(null, store.get("key-1"));
        }
    }

    @Test
    public void acceptsLatestWrite() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-1", "value-2" } });
            assertEquals("value-2", store.get("key-1"));
        }
    }

    @Test
    public void acceptsLastCompleteWrite() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-1", "value-2" } });
            tearTailRecord(store);
        }

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.get("key-1"));
        }
    }

    @Test
    public void acceptsReplicatedWrite() throws StoreInitializationException, IOException, InterruptedException,
            ExecutionException, TimeoutException {

        try (AppendOnlyLogStore leader = new AppendOnlyLogStore(LogStoreRole.LEADER)) {
            try (AppendOnlyLogStore follower = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                leader.registerFollower(follower);
                leader.put("key-1", "value-1");
                leader.internalWaitTillReplicationsComplete();

                assertEquals("value-1", follower.get("key-1"));
            }
        }

    }

    @Test
    public void rejectsWriteFromFailOver()
            throws StoreInitializationException, IOException, InterruptedException, ExecutionException,
            TimeoutException {

        try (AppendOnlyLogStore leader = new AppendOnlyLogStore(LogStoreRole.LEADER)) {
            try (AppendOnlyLogStore followerA = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                try (AppendOnlyLogStore followerB = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                    leader.registerFollower(followerA);
                    leader.registerFollower(followerB);

                    leader.put("key-1", "value-1");

                    leader.internalWaitTillReplicationsComplete();

                    assertEquals("value-1", followerA.get("key-1"));
                    assertEquals("value-1", followerB.get("key-1"));

                    followerA.becomeLeader();
                    followerA.registerFollower(followerB);

                    followerA.put("key-2", "value-2");

                    followerA.internalWaitTillReplicationsComplete();

                    assertEquals("value-2", followerB.get("key-2"));

                    assertThrows(IllegalStateException.class,
                            () -> leader.put("key-3", "value-3"));
                }
            }
        }

    }

    @Test
    public void rejectsPartitionedFollower() throws StoreInitializationException, IOException, InterruptedException,
            ExecutionException, TimeoutException {

        try (AppendOnlyLogStore leader = new AppendOnlyLogStore(LogStoreRole.LEADER)) {
            try (AppendOnlyLogStore followerA = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                try (AppendOnlyLogStore followerB = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                    leader.registerFollower(followerA);
                    leader.registerFollower(followerB);

                    leader.put("key-1", "value-1");

                    leader.internalWaitTillReplicationsComplete();

                    assertEquals("value-1", followerA.get("key-1"));
                    assertEquals("value-1", followerB.get("key-1"));

                    followerA.put("key-2", "value-2");

                    leader.internalWaitTillReplicationsComplete();

                    assertEquals("value-2", leader.get("key-2"));
                    assertEquals("value-2", followerA.get("key-2"));
                    assertEquals("value-2", followerB.get("key-2"));

                    leader.partitionFollower(followerA);

                    leader.put("key-3", "value-3");
                    leader.internalWaitTillReplicationsComplete();

                    leader.put("key-1", "value-1.1");
                    leader.internalWaitTillReplicationsComplete();

                    assertEquals(null, followerA.get("key-3"));
                    assertEquals("value-3", followerB.get("key-3"));
                    assertEquals("value-1.1", followerB.get("key-1"));
                    assertEquals("value-1", followerA.get("key-1"));

                    assertThrows(IllegalStateException.class,
                            () -> followerA.put("key-4", "value-4"),
                            () -> "ERR-PART: Unable to write value.");
                }
            }
        }

    }

    @Test
    public void ignoresIdempotentWrite() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            IdempotencyKey idempotencyKey = new IdempotencyKey(UUID.randomUUID().toString());

            long offset = store.put(idempotencyKey, "key-1", "value-1");
            long nxtOffset = store.put(idempotencyKey, "key-1", "value-1");

            assertEquals("value-1", store.get("key-1"));
            assertEquals(offset, nxtOffset);
        }
    }

    @Test
    public void rejectsInvalidIdempotentWrite() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            IdempotencyKey idempotencyKey = new IdempotencyKey(UUID.randomUUID().toString());

            store.put(idempotencyKey, "key-1", "value-1");

            assertEquals("value-1", store.get("key-1"));
            assertThrows(IllegalStateException.class, () -> {
                store.put(idempotencyKey, "key-1", "value-2");
            });
        }
    }

    @Test
    public void rejectsRecordWithoutCDCWrite() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-2", "value-2" } });

            assertEquals("value-1", store.get("key-1"));

            tearTailCDCEntry(store);
        }

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.get("key-1"));
            assertEquals(null, store.get("key-2"));
        }
    }

    @Test
    public void rejectsDuplicateOutboxEvents() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            AtomicInteger count = new AtomicInteger();

            store.registerOutboxListener(record -> {
                count.incrementAndGet();
                return null;
            });

            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-2", "value-2" } });

            store.publishOutboxEntries();
            store.publishOutboxEntries();

            assertEquals(2, count.get());
        }
    }

    @Test
    public void rejectsDuplicateOutboxEventsOnReopen() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            AtomicInteger count = new AtomicInteger();

            store.registerOutboxListener(record -> {
                count.incrementAndGet();
                return null;
            });

            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-2", "value-2" } });
            store.publishOutboxEntries();

            assertEquals(2, count.get());
        }

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            AtomicInteger count = new AtomicInteger();

            store.registerOutboxListener(record -> {
                count.incrementAndGet();
                return null;
            });

            store.publishOutboxEntries();

            assertEquals(0, count.get());
        }
    }

    @Test
    public void acceptsWriteAndReadFromQuorum() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(3)) {
            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
            assertEquals("value-1", storeClient.get("key-1"));
        }
    }

    @Test
    public void rejectsDisruptiveRebalance() {
        ConsistentHashRing hashRing = new ConsistentHashRing(100);

        for (int i = 1; i <= 3; i++) {
            hashRing.addNode("node-" + i);
        }

        Map<String, String> keyToNodeMap = new HashMap<>();

        for (int i = 1; i <= 1000; i++) {
            String key = "node#" + i;

            keyToNodeMap.put(key, hashRing.nodeForKey(key));
        }

        hashRing.addNode("node-4");

        AtomicInteger rebalancedNodes = new AtomicInteger();
        for (int i = 1; i <= 1000; i++) {
            String key = "node#" + i;

            String newNodeForKey = hashRing.nodeForKey(key);
            String oldNodeForKey = keyToNodeMap.get(key);

            if (!newNodeForKey.equals(oldNodeForKey)) {
                rebalancedNodes.incrementAndGet();
            }
        }

        assertTrue(() -> (((float) rebalancedNodes.get() / 1000) < 0.4));
    }

    @Test
    public void acceptsReadAnomalyWithoutLock()
            throws IOException, StoreInitializationException, InterruptedException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            store.put("itemsCount", "1");

            Thread t1 = newDecrementer(store, false);
            Thread t2 = newDecrementer(store, false);

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            // Read anomaly, value should ideally be -1
            assertEquals("0", store.get("itemsCount"));
        }
    }

    @Test
    public void rejectsReadAnomalyWithLock()
            throws IOException, StoreInitializationException, InterruptedException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            store.put("itemsCount", "1");
            
            Thread t1 = newDecrementer(store, true);
            Thread t2 = newDecrementer(store, true);

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            assertEquals("-1", store.get("itemsCount"));
        }
    }

    private Thread newDecrementer(AppendOnlyLogStore store, boolean lockKey) {
        return new Thread() {
            public void run() {
                try {
                    if (lockKey) {
                        store.lockKey("itemsCount");
                    }

                    int itemsCount = Integer.valueOf(store.get("itemsCount"));
                    System.out.println("Read value");

                    Thread.sleep(10);

                    store.put("itemsCount", Integer.toString(--itemsCount));
                    System.out.println("Updated value");

                    if (lockKey) {
                        store.unlockKey("itemsCount");
                    }
                } catch (Exception e) {
                    System.err.println("Error while decrementing count: " + e.getMessage());
                }
            }
        };
    }

    private void tearTailRecord(AppendOnlyLogStore store) throws IOException {
        LogStoreEntrySet lastEntry = store.getLastEntry().get();
        int endOffset = (int) (lastEntry.offset() + (lastEntry.length() / 2));

        RandomAccessFile storeFile = new RandomAccessFile(store.getFile(), "rw");

        storeFile.setLength(endOffset);
        storeFile.close();
    }

    private void tearTailCDCEntry(AppendOnlyLogStore store) throws IOException {
        LogStoreEntrySet lastEntry = store.getLastEntry().get();

        int entryEndOffset = (int) (lastEntry.offset() + lastEntry.length());
        int cdcStartOffset = entryEndOffset;

        RandomAccessFile storeFile = new RandomAccessFile(store.getFile(), "rw");

        storeFile.setLength(cdcStartOffset + LogStoreOuput.RECORD_OFFSET + 1);
        storeFile.close();
    }

    private void insertRecords(AppendOnlyLogStore store, String[][] records)
            throws StoreInitializationException, IOException {
        for (String[] record : records) {
            store.put(record[0], record[1]);
        }
    }
}
