package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

public class OutboxServiceTest {

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
    public void acceptsExactlyOnceOutboxEventsOnReopen() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            AtomicInteger count = new AtomicInteger();

            store.registerOutboxListener(record -> {
                count.incrementAndGet();
                return null;
            });

            insertRecords(store, new String[][] { { "key-1", "value-1" } });

            assertEquals(0, count.get());
        }

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            AtomicInteger count = new AtomicInteger();

            store.registerOutboxListener(record -> {
                count.incrementAndGet();
                return null;
            });

            // Publish unpublished outbox entries
            store.publishOutboxEntries();
            assertEquals(1, count.get());
            
            // A published entry must be marked as shipped
            // and not get published again i.e. the count
            // should stay intact.
            store.publishOutboxEntries();
            assertEquals(1, count.get());
        }
    }

    private void insertRecords(AppendOnlyLogStore store, String[][] records)
            throws StoreInitializationException, IOException {
        for (String[] record : records) {
            store.put(record[0], record[1]);
        }
    }

}
