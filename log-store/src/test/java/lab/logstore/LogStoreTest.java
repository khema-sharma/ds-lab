package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.RandomAccessFile;
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

            assertEquals("value-1", store.read("key-1"));
            assertEquals("value-2", store.read("key-2"));
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

            assertEquals("value-1", store.read("key-1"));
            assertEquals("value-2", store.read("key-2"));
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
            assertEquals(null, store.read("key-1"));
        }
    }

    @Test
    public void acceptsLatestWrite() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-1", "value-2" } });
            assertEquals("value-2", store.read("key-1"));
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
            assertEquals("value-1", store.read("key-1"));
        }
    }

    @Test
    public void acceptsReplicatedWrite() throws StoreInitializationException, IOException, InterruptedException,
            ExecutionException, TimeoutException {

        try (AppendOnlyLogStore leader = new AppendOnlyLogStore()) {
            try (AppendOnlyLogStore follower = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                leader.registerFollower(follower);
                leader.write("key-1", "value-1");
                leader.internalWaitTillReplicationsComplete();

                assertEquals("value-1", follower.read("key-1"));
            }
        }

    }

    @Test
    public void rejectsWriteFromFailOver()
            throws StoreInitializationException, IOException, InterruptedException, ExecutionException,
            TimeoutException {

        try (AppendOnlyLogStore leader = new AppendOnlyLogStore()) {
            try (AppendOnlyLogStore followerA = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                try (AppendOnlyLogStore followerB = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                    leader.registerFollower(followerA);
                    leader.registerFollower(followerB);

                    leader.write("key-1", "value-1");

                    leader.internalWaitTillReplicationsComplete();

                    assertEquals("value-1", followerA.read("key-1"));
                    assertEquals("value-1", followerB.read("key-1"));

                    followerA.becomeLeader();
                    followerA.registerFollower(followerB);

                    followerA.write("key-2", "value-2");

                    followerA.internalWaitTillReplicationsComplete();

                    assertEquals("value-2", followerB.read("key-2"));

                    assertThrows(IllegalStateException.class,
                            () -> leader.write("key-3", "value-3"));
                }
            }
        }

    }

    @Test
    public void rejectsPartitionedFollower() throws StoreInitializationException, IOException, InterruptedException,
            ExecutionException, TimeoutException {

        try (AppendOnlyLogStore leader = new AppendOnlyLogStore()) {
            try (AppendOnlyLogStore followerA = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                try (AppendOnlyLogStore followerB = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                    leader.registerFollower(followerA);
                    leader.registerFollower(followerB);

                    leader.write("key-1", "value-1");

                    leader.internalWaitTillReplicationsComplete();

                    assertEquals("value-1", followerA.read("key-1"));
                    assertEquals("value-1", followerB.read("key-1"));

                    followerA.write("key-2", "value-2");

                    leader.internalWaitTillReplicationsComplete();

                    assertEquals("value-2", leader.read("key-2"));
                    assertEquals("value-2", followerA.read("key-2"));
                    assertEquals("value-2", followerB.read("key-2"));

                    leader.partitionFollower(followerA);

                    leader.write("key-3", "value-3");
                    leader.internalWaitTillReplicationsComplete();

                    leader.write("key-1", "value-1.1");
                    leader.internalWaitTillReplicationsComplete();

                    assertEquals(null, followerA.read("key-3"));
                    assertEquals("value-3", followerB.read("key-3"));
                    assertEquals("value-1.1", followerB.read("key-1"));
                    assertEquals("value-1", followerA.read("key-1"));

                    assertThrows(IllegalStateException.class,
                            () -> followerA.write("key-4", "value-4"),
                            () -> "ERR-PART: Unable to write value.");
                }
            }
        }

    }

    @Test
    public void ignoresIdempotentWrite() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            IdempotencyKey idempotencyKey = new IdempotencyKey(UUID.randomUUID().toString());

            long offset = store.write(idempotencyKey, "key-1", "value-1");
            long nxtOffset = store.write(idempotencyKey, "key-1", "value-1");

            assertEquals("value-1", store.read("key-1"));
            assertEquals(offset, nxtOffset);
        }
    }

    @Test
    public void rejectsInvalidIdempotentWrite() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            IdempotencyKey idempotencyKey = new IdempotencyKey(UUID.randomUUID().toString());

            store.write(idempotencyKey, "key-1", "value-1");

            assertEquals("value-1", store.read("key-1"));
            assertThrows(IllegalStateException.class, () -> {
                store.write(idempotencyKey, "key-1", "value-2");
            });
        }
    }

    @Test
    public void rejectsRecordWithoutCDCWrite() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            insertRecords(store, new String[][] { { "key-1", "value-1" }, { "key-2", "value-2" } });

            assertEquals("value-1", store.read("key-1"));

            tearTailCDCEntry(store);
        }

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.read("key-1"));
            assertEquals(null, store.read("key-2"));
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

        storeFile.setLength(cdcStartOffset + LogStoreEntryOuput.RECORD_OFFSET + 1);
        storeFile.close();
    }

    private void insertRecords(AppendOnlyLogStore store, String[][] records)
            throws StoreInitializationException, IOException {
        for (String[] record : records) {
            store.write(record[0], record[1]);
        }
    }
}
