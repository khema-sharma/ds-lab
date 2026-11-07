package lab.ds.logstore;

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

import lab.ds.logstore.core.IdempotencyKey;

public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStoreClient.class.getSimpleName());

    private static final float QUORUM_SIZE = 0.8F;
    private static final int MINIMUM_POOL_SIZE = 3;

    private final ArrayList<NodeCircuitBreaker> nodes;
    private final int poolSize;

    private final ExecutorService quorumExecutorService;

    public AppendOnlyLogStoreClient(int size) throws StoreInitializationException, IOException {
        this(size, null);
    }

    AppendOnlyLogStoreClient(AppendOnlyLogStore[] quorum) throws StoreInitializationException, IOException {
        this(-1, quorum);
    }

    private AppendOnlyLogStoreClient(int capacity, AppendOnlyLogStore[] stores) throws StoreInitializationException {
        int size = (stores != null ? stores.length : capacity);

        if (size < MINIMUM_POOL_SIZE) {
            throw new IllegalArgumentException("Pool size must be atleast 3.");
        }

        poolSize = size;
        nodes = new ArrayList<>(poolSize);

        quorumExecutorService = Executors.newFixedThreadPool(5);

        for (int i = 0; i < poolSize; i++) {
            nodes.add(new NodeCircuitBreaker((stores == null ? new AppendOnlyLogStore() : stores[i])));
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
        put(new IdempotencyKey(UUID.randomUUID().toString()), key, value);
    }

    @Override
    public void put(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        int quorumSize = (int) (poolSize * QUORUM_SIZE);

        CountDownLatch quorumLatch = new CountDownLatch(quorumSize);

        AtomicInteger nodesAttempted = new AtomicInteger();
        AtomicInteger successfulSaves = new AtomicInteger();

        logger.info("Attempting the put operation ...");

        nodes.stream().forEach(decoraredNode -> {
            if (decoraredNode.circuitOpen()) {
                quorumLatch.countDown();
                return;
            }

            nodesAttempted.incrementAndGet();

            quorumExecutorService.submit(() -> {
                try {
                    decoraredNode.put(idempotencyKey, key, value);
                    successfulSaves.incrementAndGet();

                } catch (CircuitOpenException | OperationRuntimeException e) {
                    logger.error("An error occurred while saving the record.");
                } finally {
                    quorumLatch.countDown();
                }
            });
        });

        try {
            if (!quorumLatch.await(5, TimeUnit.SECONDS)) {
                logger.error("Quorum wait timed out for the put operation");
                throw new OperationStateUnknownException(idempotencyKey);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Quorum wait interrupted!", e);

            throw new OperationStateUnknownException(idempotencyKey);
        }

        if (nodesAttempted.get() == 0) {
            logger.error("Could noy complete the put operation, cicuits are open");
            throw new StoreBusyException();
        }

        if (successfulSaves.get() < quorumSize) {
            logger.error("Could not complete the put operation on the quorum");
            throw new IOException("Error saving the record");
        }
    }

    @Override
    public String get(String key) throws IOException {
        final List<String> valueVersions = Collections.synchronizedList(new ArrayList<>(3));
        CountDownLatch quorumLatch = new CountDownLatch(poolSize - 1);

        nodes.stream().forEach(store -> {
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
        nodes.forEach(store -> {
            try {
                store.close();
            } catch (IOException e) {
                logger.error("An error occurred while closing the store: " + e.getMessage(), e);
            }
        });
    }

    boolean allNodesClosed() {
        for (NodeCircuitBreaker node : nodes) {
            if (!node.circuitClosed()) {
                return false;
            }
        }

        return true;
    }

    void internalWaitUntilStateIsSwitched() throws InterruptedException {
        logger.info("Waiting for state latches to open ...");

        for (NodeCircuitBreaker node : nodes) {
            node.waitUntilOpen();
        }
    }

    private static class CircuitOpenException extends Exception {

        public CircuitOpenException(AppendOnlyLogStore node) {
            super();
        }

    }

    private static class OperationRuntimeException extends RuntimeException {

        public OperationRuntimeException(AppendOnlyLogStore node) {
            super("Runtime error.");
        }

    }

    private static class NodeCircuitBreaker {

        private static Logger logger = LoggerFactory.getLogger(NodeCircuitBreaker.class.getSimpleName());

        private static final int CIRCUIT_TRIP_THRESHOLD = 3;
        private static final long RETRY_IN_MILLIS = 3000;

        private enum State {
            Closed,
            HalfOpen,
            Open
        }

        private final AtomicReference<State> state;
        private final AtomicReference<CountDownLatch> stateLatch;

        private final AppendOnlyLogStore node;
        private final ScheduledExecutorService bgExecutorService;

        public NodeCircuitBreaker(AppendOnlyLogStore store) {
            node = store;

            state = new AtomicReference<State>(State.Closed);
            stateLatch = new AtomicReference<CountDownLatch>(new CountDownLatch(0));

            bgExecutorService = Executors.newScheduledThreadPool(1);
        }

        public void put(IdempotencyKey idempotencyKey, String key, String value) throws CircuitOpenException {
            if (circuitOpen()) {
                throw new CircuitOpenException(node);
            }

            int retryCount = 0;
            int maxRetries = (circuitHalfOpen() ? 1 : CIRCUIT_TRIP_THRESHOLD);

            do {
                try {
                    node.put(idempotencyKey, key, value);

                    logger.info("The put operation succeeded on the node");
                    break;

                } catch (IOException e) {
                    retryCount++;
                }
            } while (retryCount < maxRetries);

            if (retryCount == maxRetries) {
                openCircuit();
                throw new OperationRuntimeException(node);
            }

            logger.info("The put operation was successful on the quorum");

            closeCircuit();
        }

        public String get(String key) throws IOException {
            return node.get(key);
        }

        public void close() throws IOException {
            node.close();
        }

        private void waitUntilOpen() throws InterruptedException {
            stateLatch.get().await(5L, TimeUnit.SECONDS);
        }

        private boolean circuitHalfOpen() {
            return (state.get() == State.HalfOpen);
        }

        private boolean circuitOpen() {
            return (state.get() == State.Open);
        }

        private boolean circuitClosed() {
            return (state.get() == State.Closed);
        }

        private void closeCircuit() {
            logger.info("Switching to closed state");
            state.set(State.Closed);
        }

        private void halfOpenCircuit() {
            logger.info("Switching to half-open state");
            state.set(State.HalfOpen);
        }

        private void openCircuit() {
            synchronized (this) {
                logger.warn("Scheduling state reset");

                state.set(State.Open);
                stateLatch.set(new CountDownLatch(1));

                bgExecutorService.schedule(() -> {
                    halfOpenCircuit();
                    stateLatch.get().countDown();
                }, RETRY_IN_MILLIS, TimeUnit.MILLISECONDS);
            }
        }

    }

}
