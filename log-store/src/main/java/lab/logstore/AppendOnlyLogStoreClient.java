package lab.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private static final int CIRCUIT_TRIP_THRESHOLD = 3;

    private static final int CIRCUIT_CLOSED_STATE = 0;
    private static final int CIRCUIT_HALF_OPEN_STATE = 2;

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStoreClient.class);

    private final ArrayList<AppendOnlyLogStore> quorumNodes;
    private final int quorumSize;

    private final ExecutorService quorumExecutorService;
    private final ScheduledExecutorService bgExecutorService;

    private final AtomicInteger circuitBreakerCount;
    private final AtomicReference<CountDownLatch> circuitStateSwitchLatch;

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

        circuitBreakerCount = new AtomicInteger(0);
        circuitStateSwitchLatch = new AtomicReference<>(new CountDownLatch(0));

        for (int i = 0; i < quorumSize; i++) {
            quorumNodes.add(quorum == null ? new AppendOnlyLogStore() : quorum[i]);
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
     * @param key Key to be used to fetch the data
     * @param value Value to be saved in the store
     * 
     * @throws IOException if any issues in saving the record
     * @throws StoreBusyException if circuit is currently open
     */
    @Override
    public void put(String key, String value) throws IOException {
        stopIfCircuitIsOpen();

        CountDownLatch quorumLatch = new CountDownLatch(quorumSize - 1);
        List<Throwable> errors = new ArrayList<>(quorumSize);

        final IdempotencyKey idempotencyKey = new IdempotencyKey(UUID.randomUUID().toString());

        quorumNodes.stream().forEach(store -> {
            quorumExecutorService.submit(() -> {
                do {
                    try {
                        store.put(idempotencyKey, key, value);
                        quorumLatch.countDown();

                        return;

                    } catch (IOException e) {
                        if (circuitBreakerCount.get() == CIRCUIT_TRIP_THRESHOLD) {
                            errors.add(e);
                            return;
                        }

                        circuitBreakerCount.incrementAndGet();
                    }
                } while (circuitBreakerCount.get() < CIRCUIT_TRIP_THRESHOLD);
            });
        });

        try {

            quorumLatch.await(5, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            logger.error("Quorum write latch interrupted!");
        }

        if (!errors.isEmpty() && tripCircuit()) {
            throw new IOException("Error saving record.");
        }

        if (errors.isEmpty()) {
            resetCircuit();
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
                    System.err.println("An error occurred while retrieving data: " + e.getMessage());
                }
            });
        });

        try {
            quorumLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Quorum read latch interrupted.");
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
                System.err.println("An error occurred while closing the store: " + e.getMessage());
            }
        });
    }

    void internalBlockUntilCircuitIsOpen() throws InterruptedException {
        circuitStateSwitchLatch.get().await(5, TimeUnit.SECONDS);
    }

    private boolean tripCircuit() {
        synchronized (this) {
            if (circuitBreakerCount.get() < CIRCUIT_TRIP_THRESHOLD) {
                return false;
            }

            logger.warn("Tripping circuit and scheduling backend task to reset automatically after 3 seconds.");

            circuitStateSwitchLatch.set(new CountDownLatch(1));

            bgExecutorService.schedule(() -> {
                logger.info("Resetting circuit to half-open state ...");

                circuitBreakerCount.setRelease(CIRCUIT_HALF_OPEN_STATE);
                circuitStateSwitchLatch.get().countDown();
            }, 3, TimeUnit.SECONDS);

            return true;
        }
    }

    private void resetCircuit() {
        if (circuitBreakerCount.get() > CIRCUIT_CLOSED_STATE) {
            logger.info("Resetting circuit to open state ...");
            circuitBreakerCount.setRelease(CIRCUIT_CLOSED_STATE);
        }
    }

    private void stopIfCircuitIsOpen() {
        if (circuitBreakerCount.get() == CIRCUIT_TRIP_THRESHOLD) {
            logger.warn("Open circuit, won't invoke integration endpoint!");
            throw new StoreBusyException();
        }
    }

}
