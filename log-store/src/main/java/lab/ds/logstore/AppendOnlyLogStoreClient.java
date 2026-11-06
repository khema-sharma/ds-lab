package lab.ds.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lab.ds.logstore.core.IdempotencyKey;

public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private static final int MAX_RETRIES = 3;
    private static final int CIRCUIT_TRIP_THRESHOLD = 3;

    private static final int CIRCUIT_CLOSED_STATE = 0;
    private static final int CIRCUIT_HALF_OPEN_STATE = 2;

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStoreClient.class);

    private final ArrayList<AppendOnlyLogStore> quorumNodes;
    private final int quorumSize;

    private final ExecutorService quorumExecutorService;
    private final ScheduledExecutorService bgExecutorService;

    private final Map<AppendOnlyLogStore, AtomicInteger> circuitBreakerCount;
    private final Map<AppendOnlyLogStore, AtomicReference<CountDownLatch>> circuitStateSwitchLatch;

    public AppendOnlyLogStoreClient(int qSize) throws StoreInitializationException, IOException {
        this(qSize, null);
    }

    AppendOnlyLogStoreClient(AppendOnlyLogStore[] quorum) throws StoreInitializationException, IOException {
        this(-1, quorum);
    }

    private AppendOnlyLogStoreClient(int capacity, AppendOnlyLogStore[] quorum) throws StoreInitializationException {
        int qSize = (quorum != null ? quorum.length : capacity);

        if (qSize <= 2) {
            throw new IllegalArgumentException("Quorum size must be atleast 3.");
        }

        quorumSize = qSize;
        quorumNodes = new ArrayList<>(quorumSize);

        quorumExecutorService = Executors.newFixedThreadPool(5);
        bgExecutorService = Executors.newScheduledThreadPool(1);

        circuitBreakerCount = new ConcurrentHashMap<>();
        circuitStateSwitchLatch = new ConcurrentHashMap<>();

        for (int i = 0; i < quorumSize; i++) {
            AppendOnlyLogStore node = (quorum == null ? new AppendOnlyLogStore() : quorum[i]);

            quorumNodes.add(node);

            circuitBreakerCount.put(node, new AtomicInteger(CIRCUIT_CLOSED_STATE));
            circuitStateSwitchLatch.put(node, new AtomicReference<>(new CountDownLatch(0)));
        }
    }

    /**
     * Puts the key-value pair (record) in the store.
     * <p>
     * The client implements Circuit Breaker stability pattern. It opens the circuit
     * when it fails to get an answer from the downstream store after retrying three
     * times. It uses an idempotency key to protect the duplicate writes.
     * <p>
     * The client does not have an explicit solution in place to protect itself from
     * getting crushed under load using Bulkheads stability pattern. It somewhat
     * handles it by using a fixed thread pool, but still a complete solution is not
     * in place.
     * 
     * @param key   Key to be used to fetch the data
     * @param value Value to be saved in the store
     * 
     * @throws IOException        if any issues in saving the record
     * @throws StoreBusyException if circuit is currently open
     */
    @Override
    public void put(String key, String value) throws IOException {
        CountDownLatch quorumLatch = new CountDownLatch(quorumSize);

        final IdempotencyKey idempotencyKey = new IdempotencyKey(UUID.randomUUID().toString());

        final AtomicInteger numberOfNodesVisited = new AtomicInteger();
        final AtomicInteger successfulSaves = new AtomicInteger();

        quorumNodes.stream().forEach(store -> {
            quorumExecutorService.submit(() -> {
                try {
                    if (circuitOpen(store)) {
                        return;
                    }

                    numberOfNodesVisited.incrementAndGet();

                    int callCount = 0;
                    int maxRetries = (circuitHalfOpen(store) ? 1 : MAX_RETRIES);

                    do {
                        try {
                            store.put(idempotencyKey, key, value);

                            closeCicuit(store);
                            successfulSaves.incrementAndGet();

                            break;

                        } catch (IOException e) {
                            callCount++;
                            circuitBreakerCount.get(store).incrementAndGet();
                        }
                    } while (callCount < maxRetries);
                } finally {
                    quorumLatch.countDown();
                }
            });
        });

        try {
            quorumLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Quorum write latch interrupted!", e);
        }

        if (numberOfNodesVisited.get() < (quorumSize - 1)) {
            logger.warn("Circuits open, connection not allowed.");
            throw new StoreBusyException();
        }

        if (successfulSaves.get() < (quorumSize - 1)) {
            logger.error("Could not write record to the quorum.");

            scheduleResets();
            throw new IOException("Error saving record.");
        }
    }

    @Override
    public String get(String key) throws IOException {
        final List<String> valueVersions = Collections.synchronizedList(new ArrayList<>(3));
        CountDownLatch quorumLatch = new CountDownLatch(quorumSize - 1);

        quorumNodes.stream().forEach(store -> {
            quorumExecutorService.submit(() -> {
                try {
                    valueVersions.add(store.get(key));
                    quorumLatch.countDown();
                } catch (IOException e) {
                    logger.error("An error occurred while retrieving data: " + e.getMessage(), e);
                }
            });
        });

        try {
            quorumLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Quorum read latch interrupted.", e);
        }

        if (quorumLatch.getCount() > 0) {
            throw new IOException("Error retrieving data.");
        }

        return valueVersions.get(0);
    }

    @Override
    public void close() throws IOException {
        quorumNodes.forEach(store -> {
            try {
                store.close();
            } catch (IOException e) {
                logger.error("An error occurred while closing the store: " + e.getMessage(), e);
            }
        });
    }

    void internalBlockUntilCircuitIsOpen() throws InterruptedException {
        logger.info("Waiting for state latches to open ...");

        for (AppendOnlyLogStore node : quorumNodes) {
            circuitStateSwitchLatch.get(node).get().await(5, TimeUnit.SECONDS);
        }
    }

    private boolean circuitOpen(AppendOnlyLogStore node) {
        AtomicInteger counter = circuitBreakerCount.get(node);
        return (counter != null && counter.get() == CIRCUIT_TRIP_THRESHOLD);
    }

    private boolean circuitHalfOpen(AppendOnlyLogStore node) {
        AtomicInteger counter = circuitBreakerCount.get(node);
        return (counter != null && counter.get() == CIRCUIT_HALF_OPEN_STATE);
    }

    private void closeCicuit(AppendOnlyLogStore store) {
        circuitBreakerCount.get(store).set(CIRCUIT_CLOSED_STATE);
    }

    private void scheduleResets() {
        synchronized (this) {
            for (AppendOnlyLogStore node : quorumNodes) {
                if (!circuitOpen(node)) {
                    continue;
                }

                logger.warn("Scheduling reset after 3 seconds for the given node.");

                circuitStateSwitchLatch.get(node).set(new CountDownLatch(1));

                bgExecutorService.schedule(() -> {
                    logger.info("Resetting circuit to half-open state ...");

                    circuitBreakerCount.get(node).setRelease(CIRCUIT_HALF_OPEN_STATE);
                    circuitStateSwitchLatch.get(node).get().countDown();
                }, 3, TimeUnit.SECONDS);
            }
        }
    }

}
