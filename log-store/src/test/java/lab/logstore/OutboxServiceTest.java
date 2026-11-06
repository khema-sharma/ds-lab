package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import lab.logstore.core.LogStoreInsertEvent;

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

    @Test
    public void acceptsRollingCountPerKey() throws IOException, StoreInitializationException {
        Map<String, AtomicInteger> rollingCounters = new ConcurrentHashMap<>();

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            // Batch against stream for maintaining rolling counter.
            //
            // We count number of write operations per key as and when a record is saved.
            // This test case registers a listener for then events happening in the store.
            // Counter gets updated when the insert event is published by the store. It
            // maintains one counter per key.
            //
            // This is unlike a Tumbling Window update, used in batch processing, where a
            // counter is increased only when certain number of records are inserted in the
            // store.
            //
            // The rolling counter is not stateful. It gets removed when the test completes,
            // which is enough for the day 19 plan.
            store.registerOutboxListener(event -> {
                if (!(event instanceof LogStoreInsertEvent)) {
                    return null;
                }

                LogStoreInsertEvent insertEvent = (LogStoreInsertEvent) event;

                rollingCounters.compute(insertEvent.key(), (key, value) -> {
                    AtomicInteger keyCounter = (value == null ? new AtomicInteger(0) : value);
                    keyCounter.incrementAndGet();

                    return keyCounter;
                });

                return null;
            });

            store.put("key-1", "value-1");
            store.put("key-1", "value-2");

            store.publishOutboxEntries();

            assertEquals(2, rollingCounters.get("key-1").get());

            store.put("key-2", "value-2");

            store.publishOutboxEntries();

            assertEquals(2, rollingCounters.get("key-1").get());
            assertEquals(1, rollingCounters.get("key-2").get());
        }
    }

    private void insertRecords(AppendOnlyLogStore store, String[][] records)
            throws StoreInitializationException, IOException {
        for (String[] record : records) {
            store.put(record[0], record[1]);
        }
    }

}
