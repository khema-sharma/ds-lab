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
        quorumSize = (poolSize * 4) / 5;

        quorumExecutorService = createdBoundedFixedPoolExecutor();

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
     * @throws StoreBusyException             if quorum is not accessible, use
     *                                        {@code idempotencyKey} to retry put
     *                                        again
     * @throws QuorumNotReachedException      if put operation on quorum failed,
     *                                        put operation on minority could still
     *                                        succeed, use {@code idempotencyKey}
     *                                        for retrying put
     * @throws OperationStateUnknownException if quorum put status is not
     *                                        deterministic, try again with
     *                                        {@code idempotencyKey}
     */
    @Override
    public void put(String key, String value) throws IOException {
        put(new IdempotencyKey(UUID.randomUUID().toString()), key, value);
    }

    @Override
    public void put(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        resetFromLastTripping();

        logger.info("Attempting the put operation on the quorum...");
        failIfQuorumIsBusy(idempotencyKey);

        ExecutorCompletionService<Boolean> opCompletionService = new ExecutorCompletionService<>(quorumExecutorService);

        nodes.forEach(decoratedNode -> opCompletionService.submit(() -> {
            decoratedNode.put(idempotencyKey, key, value);
            return true;
        }));

        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(ResilientNode.MAX_RETRIES * ResilientNode.PUT_TIMEOUT_SECS)
                // Buffer
                + TimeUnit.SECONDS.toNanos(1);

        try {
            int successCount = 0;
            int failureCount = 0;

            int unknownStateErrors = 0;
            int openCircuitErrors = 0;

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
                    } else if (futureOp.exceptionNow() instanceof CircuitOpenException) {
                        openCircuitErrors++;
                    }

                    failureCount++;
                }
            }

            if (successCount < quorumSize) {
                logger.error("put operation failed, did not get the confirmation from the quorum");

                if (unknownStateErrors > 0) {
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Unable to determine the state of the put operation");
                }

                if (successCount == 0 && openCircuitErrors == failureCount) {
                    throw new StoreBusyException(idempotencyKey);
                }

                throw new QuorumNotReachedException("Failed to save record", idempotencyKey);
            }

            logger.info("put operation on the quorum was successful");
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

    private void failIfQuorumIsBusy(IdempotencyKey idempotencyKey) throws StoreBusyException {
        long totalAccessible = nodes.stream().filter(node -> node.accessible()).count();

        if (totalAccessible < quorumSize) {
            logger.warn("Quorum has been cut off, backing off...");
            throw new StoreBusyException(idempotencyKey);
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

    void internalWaitUntilStateSwitchThreshold() {
        nodes.forEach(node -> node.internalTickLastTrippedBackwardsBy(3));
    }

    private static class CircuitOpenException extends Exception {

        public CircuitOpenException(AppendOnlyLogStore node) {
            super();
        }

    }

    private ExecutorService createdBoundedFixedPoolExecutor() {
            return Executors.newFixedThreadPool(5);
            // return new ThreadPoolExecutor(quorumSize, poolSize, 60L, TimeUnit.SECONDS,
            //         new ArrayBlockingQueue<>(quorumSize), new ThreadPoolExecutor.AbortPolicy());
        }

    private static class OperationRuntimeException extends RuntimeException {

        public OperationRuntimeException(AppendOnlyLogStore node, String message, List<Throwable> causes) {
            super(message);
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
        private static final long RESET_TIME_INTERVAL = TimeUnit.SECONDS.toNanos(3);
        private static final int PUT_TIMEOUT_SECS = 3;

        private enum State {
            Closed, HalfOpen, Probing, Open
        }

        private record CurrentState(State state, long openedAt) {
        }

        private final ExecutorService putExecutor;
        private final AppendOnlyLogStore node;
        private final AtomicReference<CurrentState> currentState;

        public ResilientNode(AppendOnlyLogStore store) {
            node = store;
            currentState = new AtomicReference<CurrentState>(new CurrentState(State.Closed, Long.MIN_VALUE));

            putExecutor = Executors.newFixedThreadPool(5);
        }

        public void put(IdempotencyKey idempotencyKey, String key, String value)
                throws CircuitOpenException, IOException {
            logger.info(" attempting put on " + node.id() + "...");

            switch (currentState.get().state()) {
                case Open, Probing -> throw new CircuitOpenException(node);
                case HalfOpen -> probeOnce(idempotencyKey, key, value);
                case Closed -> putWithRetries(idempotencyKey, key, value);
            }
        }

        public String get(String key) throws IOException {
            return node.get(key);
        }

        public void close() throws IOException {
            node.close();
            putExecutor.shutdownNow();
        }

        private void probeOnce(IdempotencyKey idempotencyKey, String key, String value)
                throws CircuitOpenException, IOException {
            if (!transitionState(State.HalfOpen, State.Probing)) {
                logger.error(node.id() + " could not be probed, some other thread has already started the probing");
                throw new CircuitOpenException(node);
            }

            try {
                putWithTimeout(idempotencyKey, key, value);
                transitionState(State.Probing, State.Closed);

            } finally {
                transitionState(State.Probing, State.Open);
            }
        }

        private void resetCircuit() {
            CurrentState cState = currentState.get();
            boolean timeLapsed = (System.nanoTime() - cState.openedAt()) >= RESET_TIME_INTERVAL;

            if (cState.state() == State.Open && timeLapsed) {
                currentState.compareAndSet(cState, new CurrentState(State.HalfOpen, cState.openedAt()));
            }
        }

        private void putWithRetries(IdempotencyKey idempotencyKey, String key, String value)
                throws CircuitOpenException, IOException {
            int retryCount = 0;
            int unknowStateErrors = 0;

            List<Throwable> opExceptions = new ArrayList<>();

            do {
                try {
                    // Could get opened by other threads
                    if (!circuitClosed()) {
                        throw new CircuitOpenException(node);
                    }

                    putWithTimeout(idempotencyKey, key, value);

                    logger.info("put operation completed successfully on " + node.id());
                    break;

                } catch (IOException e) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw e;
                    }

                    opExceptions.add(e);
                    retryCount++;

                    if (e instanceof OperationStateUnknownException) {
                        unknowStateErrors++;
                    }
                }
            } while (retryCount < MAX_RETRIES);

            if (retryCount >= CIRCUIT_TRIP_THRESHOLD) {
                logger.error("put operation failed on node " + node.id() + ", opening its circuit");
                transitionState(State.Closed, State.Open);
            }

            if (retryCount == MAX_RETRIES) {
                if (unknowStateErrors > 0) {
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Could not detemine the state of the put operation on node " + node.id());
                }

                String err = "put operation on " + node.id() + " failed";
                throw new OperationRuntimeException(node, err, opExceptions);
            }
        }

        private long putWithTimeout(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
            Future<Long> opFuture = putExecutor.submit(() -> {
                return node.put(idempotencyKey, key, value);
            });

            try {
                return opFuture.get(PUT_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                opFuture.cancel(true);
                Thread.currentThread().interrupt();

                throw new OperationStateUnknownException(idempotencyKey, e);

            } catch (TimeoutException e) {
                opFuture.cancel(true);
                throw new OperationStateUnknownException(idempotencyKey, e);

            } catch (ExecutionException e) {
                if (e.getCause() instanceof IOException io) {
                    throw io;
                }

                if (e.getCause() instanceof RuntimeException re) {
                    throw re;
                }

                throw new IOException(e);
            }
        }

        private void internalTickLastTrippedBackwardsBy(int ticksInSeconds) {
            CurrentState cState = currentState.get();

            long tripTime = cState.openedAt();
            if (tripTime == Long.MIN_VALUE) {
                return;
            }

            long pushedBackTime = (tripTime - TimeUnit.SECONDS.toNanos(ticksInSeconds));

            currentState.compareAndSet(cState, new CurrentState(cState.state(), pushedBackTime));
        }

        private boolean circuitClosed() {
            return currentState.get().state() == State.Closed;
        }

        private boolean accessible() {
            return switch (currentState.get().state()) {
                case Closed, HalfOpen -> true;
                case Open, Probing -> false;
            };
        }

        private boolean transitionState(State from, State to) {
            while (true) {
                CurrentState cState = currentState.get();

                if (cState.state() != from) {
                    return false;
                }

                long openedAt = (to == State.Open ? System.nanoTime() : cState.openedAt());

                if (currentState.compareAndSet(cState, new CurrentState(to, openedAt))) {
                    return true;
                }
            }
        }

    }

}
