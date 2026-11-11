package lab.ds.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lab.ds.logstore.core.IdempotencyKey;

/**
 * Client that puts and gets from a quorum of {@code AppendOnlyLogStore} nodes.
 * 
 */
public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStoreClient.class.getSimpleName());

    static final int MINIMUM_POOL_SIZE = 3;
    static final long QUORUM_TIMOUT_NANOS;

    static {
        // The putWithRetries will be capped atmost by (PUT_TASK_EXPIRT_NANOS *
        // MAX_RETRIES) ns.
        //
        // For each node this time is spent in parallel
        QUORUM_TIMOUT_NANOS = (ResilientNode.MAX_RETRIES * ResilientNode.PUT_TASK_EXPIRY_NANOS)
                // Buffer
                + TimeUnit.SECONDS.toNanos(1);
    }

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
     * Generates an idempotency key and calls
     * {@link #put(IdempotencyKey, String, String)}.
     * 
     * @param key   Key to be used to fetch the data
     * @param value Value to be saved in the store
     * 
     */
    @Override
    public void put(String key, String value) throws RetryableStoreException {
        put(new IdempotencyKey(UUID.randomUUID().toString()), key, value);
    }

    @Override
    public void put(IdempotencyKey idempotencyKey, String key, String value) throws RetryableStoreException {
        if (idempotencyKey == null) {
            throw new IllegalArgumentException("idempotencyKey cannot be null.");
        }

        resetCircuits();

        logger.info("Attempting the put operation on the quorum...");
        failIfQuorumIsBusy(idempotencyKey);

        ExecutorCompletionService<Boolean> opCompletionService = new ExecutorCompletionService<>(quorumExecutorService);

        long deadline = System.nanoTime() + QUORUM_TIMOUT_NANOS;

        nodes.forEach(decoratedNode -> opCompletionService.submit(() -> {
            decoratedNode.put(idempotencyKey, key, value);
            return true;
        }));

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
                    Throwable executionException = futureOp.exceptionNow();

                    if (executionException instanceof ExecutionException) {
                        executionException = executionException.getCause();
                    }

                    if (executionException instanceof OperationStateUnknownException) {
                        // An unknown state is neither counts as a success nor as a failure
                        unknownStateErrors++;
                    } else {
                        failureCount++;

                        if (executionException instanceof CircuitOpenException) {
                            openCircuitErrors++;
                        }
                    }
                }
            }

            if (successCount < quorumSize) {
                logger.error("The put operation failed, did not get the confirmation from the quorum");

                if (unknownStateErrors > 0) {
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Unable to determine the state of the put operation");
                }

                if (successCount == 0 && openCircuitErrors == poolSize) {
                    throw new StoreBusyException(idempotencyKey);
                }

                throw new QuorumNotReachedException(idempotencyKey, "Failed to save record");
            }

            logger.info(" put operation on the quorum was successful");
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

    private void resetCircuits() {
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
        nodes.forEach(node -> node.internalTickLastTrippedBackwardsBy(9));
    }

    private ExecutorService createdBoundedFixedPoolExecutor() {
        return Executors.newFixedThreadPool(5);
    }

    private static class CircuitOpenException extends Exception {

        public CircuitOpenException(AppendOnlyLogStore node) {
            super();
        }

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

        private static final long RESET_TIME_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(3 * MAX_RETRIES);
        private static final long PUT_TASK_EXPIRY_NANOS = TimeUnit.SECONDS.toNanos(3);
        private static final long PUT_TASK_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

        private enum State {
            Closed, HalfOpen, Probing, Open
        }

        private AtomicInteger retryCounter = new AtomicInteger();

        private record RunningPutOperation(CompletableFuture<Long> future, long since) {

            public static RunningPutOperation newCompleteableOperation() {
                return new RunningPutOperation(new CompletableFuture<Long>(), System.nanoTime());
            }

            public static RunningPutOperation newCompletedOperation(long result, RunningPutOperation runningOp) {
                return new RunningPutOperation(CompletableFuture.completedFuture(result),
                        runningOp != null ? runningOp.since() : Long.MIN_VALUE);
            }

            public boolean expired() {
                return (System.nanoTime() - since) >= PUT_TASK_EXPIRY_NANOS;
            }

        }

        private record CurrentState(State state, long openedAt) {
        }

        private final ExecutorService putExecutor;
        private final AppendOnlyLogStore node;
        private final AtomicReference<CurrentState> currentState;

        private final Map<IdempotencyKey, RunningPutOperation> runningOperations;

        public ResilientNode(AppendOnlyLogStore store) {
            node = store;

            currentState = new AtomicReference<CurrentState>(new CurrentState(State.Closed, Long.MIN_VALUE));
            putExecutor = Executors.newFixedThreadPool(5);
            runningOperations = new ConcurrentHashMap<>();
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

            logger.info(" " + node.id() + " will be probed");

            try {
                putWithTimeout(idempotencyKey, key, value);
                transitionState(State.Probing, State.Closed);

            } catch (Exception e) {
                transitionState(State.Probing, State.Open);

                throw e;
            }
        }

        private void resetCircuit() {
            CurrentState cState = currentState.get();
            boolean timeLapsed = (System.nanoTime() - cState.openedAt()) >= RESET_TIME_INTERVAL_NANOS;

            if (cState.state() == State.Open && timeLapsed) {
                if (currentState.compareAndSet(cState, new CurrentState(State.HalfOpen, cState.openedAt()))) {
                    logger.info(node.id() + "'s' circuit was reset");
                }
            }
        }

        private void putWithRetries(IdempotencyKey idempotencyKey, String key, String value)
                throws CircuitOpenException, IOException {
            int unknowStateErrors = 0;

            List<Throwable> opExceptions = new ArrayList<>();

            runningOperations.put(idempotencyKey,
                    new RunningPutOperation(CompletableFuture.completedFuture(-1L), System.nanoTime()));

            do {
                try {
                    // Could get opened by other threads
                    if (!circuitClosed()) {
                        throw new CircuitOpenException(node);
                    }

                    if (Thread.currentThread().isInterrupted()) {
                        throw new OperationStateUnknownException(idempotencyKey,
                                "Put retry was interrupted for node " + node.id() + ", cannot determine its state");
                    }

                    putWithTimeout(idempotencyKey, key, value);

                    logger.info(" put operation completed successfully on " + node.id());
                    break;

                } catch (IOException e) {
                    opExceptions.add(e);

                    if (!(e instanceof PendingPutOperationException)) {
                        retryCounter.incrementAndGet();
                    }

                    if (e instanceof OperationStateUnknownException) {
                        unknowStateErrors++;
                    }

                    if (retryCounter.get() < MAX_RETRIES) {
                        backOffExponentially(retryCounter.get());
                    }
                } catch (CircuitOpenException e) {
                    throw e;

                } catch (RuntimeException e) {
                    transitionState(State.Closed, State.Open);
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Could not detemine the state of the put operation on node " + node.id());
                }
            } while (retryCounter.get() < MAX_RETRIES);

            runningOperations.remove(idempotencyKey);

            if (retryCounter.get() >= CIRCUIT_TRIP_THRESHOLD) {
                logger.error(" put operation failed on node " + node.id() + ", opening its circuit");
                transitionState(State.Closed, State.Open);
            }

            if (retryCounter.get() == MAX_RETRIES) {
                retryCounter.set(0);

                if (unknowStateErrors > 0) {
                    throw new OperationStateUnknownException(idempotencyKey,
                            "Could not detemine the state of the put operation on node " + node.id());
                }

                String err = "put operation on " + node.id() + " failed";
                throw new OperationRuntimeException(node, err, opExceptions);
            }
        }

        private long putWithTimeout(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
            Long lastResult = getLastPutResult(idempotencyKey, key, value);

            if (lastResult != null) {
                logger.info(" reusing result of last slow put with idempotency key " + idempotencyKey.key()
                        + " on node " + node.id());

                return lastResult;
            }

            runningOperations.put(idempotencyKey, RunningPutOperation.newCompleteableOperation());

            Future<Long> opFuture = putExecutor.submit(() -> {
                if (Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException(
                            "Put thread was interrupted on node " + node.id() + ", aborting...");
                }

                long offset = node.put(idempotencyKey, key, value);

                return completePutOperation(idempotencyKey, offset);
            });

            logger.trace("  submitted timed put for node" + node.id());

            try {
                return opFuture.get(PUT_TASK_TIMEOUT_NANOS, TimeUnit.NANOSECONDS);

            } catch (InterruptedException e) {
                opFuture.cancel(true);
                Thread.currentThread().interrupt();

                throw new OperationStateUnknownException(idempotencyKey, e);

            } catch (TimeoutException e) {
                logger.info(
                        " operation timed out in node " + node.id() + ", submitted task may complete in the future");

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

        private long completePutOperation(IdempotencyKey idempotencyKey, long result) {
            RunningPutOperation existingOperation = runningOperations.get(idempotencyKey);
            runningOperations.put(idempotencyKey, RunningPutOperation.newCompletedOperation(result, existingOperation));

            return result;
        }

        private Long getLastPutResult(IdempotencyKey idempotencyKey, String key, String value)
                throws PendingPutOperationException {
            RunningPutOperation runnnigPutOp = runningOperations.get(idempotencyKey);

            if (runnnigPutOp != null) {
                CompletableFuture<Long> completablePut = runnnigPutOp.future();

                if (!completablePut.isDone() && !runnnigPutOp.expired()) {
                    logger.trace(" rejected the retry, allowing previous attempt to complete");
                    throw new PendingPutOperationException(
                            "Won't submit a new put task until the existing one completes");

                } else if (completablePut.isDone()) {
                    try {
                        long lastResult = completablePut.getNow(-1L);

                        if (lastResult != -1) {
                            return lastResult;
                        }
                    } catch (Exception e) {
                        Throwable cause = (e instanceof CancellationException) ? e : e.getCause();

                        logger.error(" last put either got canceled or ended with an exception for node "
                                + node.id() + ", new put task will be submitted: " + cause.getMessage());
                    }
                }
            }

            return null;
        }

        private void backOffExponentially(int retryCount) {
            try {
                long delay = TimeUnit.NANOSECONDS.toMillis(PUT_TASK_TIMEOUT_NANOS);
                long jitteredDelay = ThreadLocalRandom.current().nextLong(delay / 4, delay / 3);

                logger.trace(" holding retries for " + jitteredDelay + " ms for node " + node.id());

                Thread.sleep(jitteredDelay);

            } catch (InterruptedException ie) {
                logger.error(" interrupted while backing off put for node " + node.id());
                Thread.currentThread().interrupt();
            }
        }

    }

}
