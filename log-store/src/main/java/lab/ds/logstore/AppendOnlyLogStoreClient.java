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
import java.util.concurrent.TimeoutException;
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

        logger.info("Attempting the put operation on the quorum...");
        failIfQuorumIsBusy();

        ExecutorCompletionService<Boolean> opCompletionService = new ExecutorCompletionService<>(quorumExecutorService);

        nodes.forEach(decoratedNode -> opCompletionService.submit(() -> {
            decoratedNode.put(idempotencyKey, key, value);
            return true;
        }));

        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(nodes.size() * ResilientNode.MAX_RETRIES * ResilientNode.PUT_TIMEOUT_SECS);

        try {
            int successCount = 0;
            int failureCount = 0;

            int unknownStateErrors = 0;

            while (successCount < quorumSize && (poolSize - failureCount) >= quorumSize) {
                Future<Boolean> futureOp = opCompletionService.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);

                if (futureOp == null) {
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Timeout while awaiting response from quorum");
                }

                if (succeededOperation(futureOp)) {
                    successCount++;
                } else {
                    if (futureOp.exceptionNow() instanceof OperationStateUnknownException) {
                        unknownStateErrors++;
                    }

                    failureCount++;
                }
            }

            if (successCount < quorumSize) {
                logger.error("The put operation failed, did not get confirmation from the quorum");

                if (unknownStateErrors > 0) {
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Unable to determine the state of the put operation.");
                }

                throw new IOException("Failed to save record");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OperationStateUnknownException(idempotencyKey, e);
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

        quorumExecutorService.shutdownNow();
    }

    private void resetFromLastTripping() {
        nodes.forEach(node -> node.resetCircuit());
    }

    private void failIfQuorumIsBusy() throws StoreBusyException {
        long totalAccessible = nodes.stream().filter(node -> !node.circuitOpen()).count();

        if (totalAccessible < quorumSize) {
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

    void internalWaitUntilStateSwitchThreshold() throws InterruptedException {
        nodes.forEach(node -> node.internalTickLastTrippedBackwardsBy(3));
    }

    private static class CircuitOpenException extends Exception {

        public CircuitOpenException(AppendOnlyLogStore node) {
            super();
        }

    }

    private static class OperationRuntimeException extends RuntimeException {

        public OperationRuntimeException(AppendOnlyLogStore node, List<Throwable> causes) {
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
        private static final long RESET_TIME_INTERVAL_MILLIS = 3000;
        private static final int PUT_TIMEOUT_SECS = 3;

        private final ExecutorService putExecutors;
        private final ExecutorCompletionService<Long> putCompletionService;

        private enum State {
            Closed, HalfOpen, Probing, Open
        }

        private final AtomicReference<State> state;
        private final AppendOnlyLogStore node;

        private final AtomicLong lastTrippedTime;

        public ResilientNode(AppendOnlyLogStore store) {
            node = store;
            state = new AtomicReference<State>(State.Closed);

            lastTrippedTime = new AtomicLong(Long.MIN_VALUE);

            putExecutors = Executors.newCachedThreadPool();
            putCompletionService = new ExecutorCompletionService<>(putExecutors);
        }

        public void put(IdempotencyKey idempotencyKey, String key, String value) throws CircuitOpenException {
            if (circuitOpen() || circuitUnderProbe()) {
                logger.error("Won't call the put operation on node " + node.id()
                        + ", its circuit is either open or under probe by some other thread");
                throw new CircuitOpenException(node);
            }

            if (circuitHalfOpen()) {
                probeOnce(idempotencyKey, key, value);
                return;
            }

            putWithRetries(idempotencyKey, key, value);
        }

        public String get(String key) throws IOException {
            return node.get(key);
        }

        public void close() throws IOException {
            node.close();
        }

        private void probeOnce(IdempotencyKey idempotencyKey, String key, String value)
                throws CircuitOpenException {

            if (!probeCircuit()) {
                logger.error(node.id() + " could not be probed, some other thread has already started the probing");
                throw new CircuitOpenException(node);
            }

            try {
                putWithTimebound(idempotencyKey, key, value);

                logger.error("Probe succeeded for node " + node.id() + ", closing its circuit");
                closeCircuit();

            } catch (IOException e) {
                throw new OperationRuntimeException(node, List.of(e));
            }

        }

        private synchronized void resetCircuit() {
            long tripTime = lastTrippedTime.get();

            if (tripTime == Long.MIN_VALUE) {
                return;
            }

            long currentTime = System.nanoTime();
            long timeLapse = TimeUnit.NANOSECONDS.toMillis(currentTime - lastTrippedTime.get());

            if (timeLapse >= RESET_TIME_INTERVAL_MILLIS) {
                halfOpenCircuit();
                lastTrippedTime.set(Long.MIN_VALUE);
            }
        }

        private void putWithRetries(IdempotencyKey idempotencyKey, String key, String value)
                throws CircuitOpenException {

            int retryCount = 0;
            int unknowStateErrors = 0;

            List<Throwable> opExceptions = new ArrayList<>();

            do {
                try {
                    // Could get opened by other threads
                    if (circuitOpen()) {
                        throw new CircuitOpenException(node);
                    }

                    putWithTimebound(idempotencyKey, key, value);

                    break;

                } catch (Exception e) {
                    opExceptions.add(e);
                    retryCount++;

                    if (e instanceof OperationStateUnknownException) {
                        unknowStateErrors++;
                    }
                }
            } while (retryCount < MAX_RETRIES);

            if (retryCount >= CIRCUIT_TRIP_THRESHOLD) {
                logger.error("The put operation failed on node " + node.id() + ", opening its circuit");
                openCircuit(State.Closed);
            }

            if (retryCount == MAX_RETRIES) {
                if (unknowStateErrors > 0) {
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Could not detemine the state of the put operation on node " + node.id());
                }

                throw new OperationRuntimeException(node, opExceptions);
            }

            logger.info("The put operation was successful on node " + node.id() + ", closing its circuit");
            closeCircuit();

        }

        private long putWithTimebound(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
            try {
                return putCompletionService.submit(() -> {
                    try {
                        return node.put(idempotencyKey, key, value);
                    } catch (IOException e) {
                        openCircuit(State.Probing);

                        throw e;
                    }
                }).get(PUT_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                throw new OperationStateUnknownException(idempotencyKey, e);

            } catch (ExecutionException e) {
                throw (IOException) e.getCause();
            }
        }

        private void internalTickLastTrippedBackwardsBy(int ticksInSeconds) {
            long tripTime = lastTrippedTime.get();
            if (tripTime == Long.MIN_VALUE) {
                return;
            }

            lastTrippedTime.set(tripTime - TimeUnit.SECONDS.toNanos(ticksInSeconds));
        }

        private boolean circuitHalfOpen() {
            return (state.get() == State.HalfOpen);
        }

        private boolean circuitUnderProbe() {
            return (state.get() == State.Probing);
        }

        private boolean circuitOpen() {
            return (state.get() == State.Open);
        }

        private boolean probeCircuit() {
            return state.compareAndSet(State.HalfOpen, State.Probing);
        }

        private boolean closeCircuit() {
            return state.compareAndSet(State.Probing, State.Closed);
        }

        private boolean halfOpenCircuit() {
            return state.compareAndSet(State.Open, State.HalfOpen);
        }

        private boolean openCircuit(State expectedState) {
            boolean transitioned = state.compareAndSet(expectedState, State.Open);

            if (transitioned) {
                lastTrippedTime.set(System.nanoTime());
            }

            return transitioned;
        }

    }

}
