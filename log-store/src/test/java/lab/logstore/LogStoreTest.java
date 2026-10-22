package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

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
    public void acceptsReplicatedWrite() throws StoreInitializationException, IOException, InterruptedException {
        try (AppendOnlyLogStore leader = new AppendOnlyLogStore()) {
            try (AppendOnlyLogStore follower = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                leader.registerFollower(follower);
                leader.write("key-1", "value-1");

                Thread.sleep(1000);
                assertEquals("value-1", follower.read("key-1"));
            }
        }
    }

    @Test
    public void rejectsWriteFromFailOver()
            throws StoreInitializationException, IOException, InterruptedException {
        try (AppendOnlyLogStore leader = new AppendOnlyLogStore()) {
            try (AppendOnlyLogStore followerA = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                try (AppendOnlyLogStore followerB = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                    leader.registerFollower(followerA);
                    leader.registerFollower(followerB);

                    leader.write("key-1", "value-1");

                    Thread.sleep(200);

                    assertEquals("value-1", followerA.read("key-1"));
                    assertEquals("value-1", followerB.read("key-1"));

                    followerA.becomeLeader();
                    followerA.registerFollower(followerB);

                    followerA.write("key-2", "value-2");

                    Thread.sleep(200);

                    assertEquals("value-2", followerB.read("key-2"));

                    assertThrows(IllegalStateException.class,
                            () -> leader.write("key-3", "value-3"));
                }
            }
        }
    }

    @Test
    public void rejectsPartitionedFollower() throws StoreInitializationException, IOException, InterruptedException {
        try (AppendOnlyLogStore leader = new AppendOnlyLogStore()) {
            try (AppendOnlyLogStore followerA = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                try (AppendOnlyLogStore followerB = new AppendOnlyLogStore(LogStoreRole.FOLLOWER)) {
                    leader.registerFollower(followerA);
                    leader.registerFollower(followerB);

                    leader.write("key-1", "value-1");

                    Thread.sleep(200);

                    assertEquals("value-1", followerA.read("key-1"));
                    assertEquals("value-1", followerB.read("key-1"));

                    followerA.write("key-2", "value-2");

                    Thread.sleep(200);

                    assertEquals("value-2", leader.read("key-2"));
                    assertEquals("value-2", followerA.read("key-2"));
                    assertEquals("value-2", followerB.read("key-2"));

                    leader.partitionFollower(followerA);

                    leader.write("key-3", "value-3");
                    leader.write("key-1", "value-1.1");

                    Thread.sleep(800);

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
    public void rejectsDuplicateWrite() throws StoreInitializationException, IOException {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            IdempotencyKey idempotencyKey = new IdempotencyKey(UUID.randomUUID().toString());

            long offset = store.write(idempotencyKey, "key-1", "value-1");
            long nxtOffset = store.write(idempotencyKey, "key-2", "value-2");

            assertEquals("value-1", store.read("key-1"));
            assertEquals(offset, nxtOffset);
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

    private void tearTailRecord(AppendOnlyLogStore store) throws IOException {
        LogStoreEntry lastEntry = store.getLastEntry().get();
        int endOffset = (int) lastEntry.offset() + (lastEntry.toString().length() - (lastEntry.length()/2));

        Path storePathObject = Path.of(URI.create("file:///" + store.getPath()));

        String contents = Files.readString(storePathObject, StandardCharsets.UTF_8);
        String tornContents = contents.substring(0, endOffset);
        
        Files.writeString(storePathObject, tornContents, StandardCharsets.UTF_8);
    }

    private void tearTailCDCEntry(AppendOnlyLogStore store) throws IOException {
        LogStoreEntry lastEntry = store.getLastEntry().get();
        int endOffset = (int) (lastEntry.offset() + lastEntry.toString().length()) + 1;

        Path storePathObject = Path.of(URI.create("file:///" + store.getPath()));

        String contents = Files.readString(storePathObject, StandardCharsets.UTF_8);
        String tornContents = contents.substring(0, endOffset);
        
        Files.writeString(storePathObject, tornContents, StandardCharsets.UTF_8);
    }

    private void insertRecords(AppendOnlyLogStore store, String[][] records)
            throws StoreInitializationException, IOException {
        for (String[] record : records) {
            store.write(record[0], record[1]);
        }
    }
}
