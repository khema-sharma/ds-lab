package lab.ds.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lab.ds.logstore.core.IdempotencyKey;

public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStoreClient.class.getSimpleName());

    private static final int MINIMUM_POOL_SIZE = 3;

    private final ArrayList<ResilientNode> nodes;
    private final int poolSize;
    private final int quorumSize;

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
        quorumSize = (int) (poolSize * 4) / 5;

        quorumExecutorService = Executors.newFixedThreadPool(5);

        nodes = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            nodes.add(new ResilientNode((stores == null ? new AppendOnlyLogStore() : stores[i])));
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
        resetFromLastTripping();

        logger.info("Attempting the put operation...");
        failIfQuorumIsBusy();

        ExecutorCompletionService<Boolean> opCompletionService = new ExecutorCompletionService<>(quorumExecutorService);

        int successCount = 0;
        int failureCount = 0;

        nodes.forEach(decoratedNode -> opCompletionService.submit(() -> {
            try {
                decoratedNode.put(idempotencyKey, key, value);
                return true;

            } catch (CircuitOpenException | OperationRuntimeException e) {
                return false;
            }
        }));

        long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

        try {
            while (successCount < quorumSize && (poolSize - failureCount) >= quorumSize) {
                if (succeededOperation(opCompletionService.poll(timeout, TimeUnit.NANOSECONDS))) {
                    successCount++;
                } else {
                    failureCount++;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OperationStateUnknownException(idempotencyKey);
        }

        if (successCount < quorumSize) {
            logger.error("The put operation failed, did not get confirmation from the quorum");
            throw new IOException("Failed to save record");
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

    private void resetFromLastTripping() {
        nodes.forEach(node -> node.resetCircuit());
    }

    private void failIfQuorumIsBusy() throws StoreBusyException {
        long totalOpen = nodes.stream().filter(node -> node.circuitOpen()).count();

        if (totalOpen >= quorumSize) {
            logger.error("Quorum has been cut off, backing off...");
            throw new StoreBusyException();
        }
    }

    private boolean succeededOperation(Future<Boolean> future) {
        Boolean state = null;

        try {
            state = future.get();
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }

        return state;
    }

    boolean allNodesClosed() {
        for (ResilientNode node : nodes) {
            if (!node.circuitClosed()) {
                return false;
            }
        }

        return true;
    }

    void internalWaitUntilStateSwitchThreshold() throws InterruptedException {
        Thread.sleep(ResilientNode.CIRCUIT_TRIP_THRESHOLD * 1000);
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

    /**
     * ResilientNode wraps an AppendOnlyStore to add resiliency to its operations.
     * <p>
     * It uses Circuit Breaker pattern to protect the downstream store from getting
     * overloaded by tripping the circuit whenever it gets consecutive errors.
     * <p>
     * Before considering the downstream store as "loaded", it reties the operations
     * multiple times to see if the errors are transient. In its current shape the
     * retries are not backedoff incremently, they happen immediately.
     */
    private static class ResilientNode {

        private static Logger logger = LoggerFactory.getLogger(ResilientNode.class.getSimpleName());

        private static final int CIRCUIT_TRIP_THRESHOLD = 3;
        private static final int MAX_RETRIES = 3;

        private enum State {
            Closed,
            HalfOpen,
            Open
        }

        private final AtomicReference<State> state;
        private final AppendOnlyLogStore node;
        private final AtomicLong lastTrippedTime;

        public ResilientNode(AppendOnlyLogStore store) {
            node = store;
            state = new AtomicReference<State>(State.Closed);
            lastTrippedTime = new AtomicLong();
        }

        public void put(IdempotencyKey idempotencyKey, String key, String value) throws CircuitOpenException {
            if (circuitOpen()) {
                throw new CircuitOpenException(node);
            }

            boolean halfOpen = circuitHalfOpen();

            int retryCount = 0;
            int maxRetries = (halfOpen ? 1 : MAX_RETRIES);

            do {
                try {
                    // Could get opened by other threads
                    if (circuitOpen()) {
                        break;
                    }

                    node.put(idempotencyKey, key, value);

                    logger.info("The put operation succeeded on the node");
                    break;

                } catch (IOException e) {
                    retryCount++;
                }
            } while (retryCount < maxRetries);

            if ((halfOpen && retryCount > 0) || (retryCount >= CIRCUIT_TRIP_THRESHOLD)) {
                openCircuit(halfOpen);
            }

            if (retryCount == maxRetries) {
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

        public synchronized void resetCircuit() {
            long currentTime = System.nanoTime();

            if (lastTrippedTime.get() <= 0) {
                return;
            }

            long timeLapse = TimeUnit.NANOSECONDS.toSeconds(currentTime - lastTrippedTime.get());
            if (timeLapse >= CIRCUIT_TRIP_THRESHOLD) {
                halfOpenCircuit();
                lastTrippedTime.set(0);
            }
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
            state.compareAndSet(State.HalfOpen, State.Closed);
        }

        private void halfOpenCircuit() {
            logger.info("Switching to half-open state");
            state.compareAndSet(State.Open, State.HalfOpen);
        }

        private void openCircuit(boolean halfOpen) {
            synchronized (this) {
                logger.warn("Switching to open state");

                State expectedState = (halfOpen ? State.HalfOpen : State.Closed);
                state.compareAndSet(expectedState, State.Open);

                lastTrippedTime.set(System.nanoTime());
            }
        }

    }

}
