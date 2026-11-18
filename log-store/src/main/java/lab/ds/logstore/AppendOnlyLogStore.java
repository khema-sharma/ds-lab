package lab.ds.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lab.ds.logstore.core.AbstractStore;
import lab.ds.logstore.core.IdempotencyKey;
import lab.ds.logstore.core.LogStoreEvent;
import lab.ds.logstore.core.LogStoreInsertEvent;
import lab.ds.logstore.core.OperationBenchmark;
import lab.ds.logstore.outbox.OutboxStore;
import lab.ds.logstore.replication.ReplicationState;

/**
 * AppendOnlyLogStore is a simple {@code(key, value)} store backed up by a
 * single database file.
 * <p>
 * To create an instance of the store, one of the following constructors can be
 * used:
 * <ul>
 * <li>The {@link #AppendOnlyLogStore()} creates a store backed up by a
 * temporary file
 * <li>The {@link #AppendOnlyLogStore(String)} creates a stored backed up by the
 * file located at given {@code path}
 * </ul>
 * <p>
 * Following methods allow to work with the store:
 * <ul>
 * <li>The {@link #put(String, String)} method writes the given {@code(key,
 * value)} pair to the store
 * <li>The {@link #get(String)} method return the value of the given
 * {@code key} from the store
 * </ul>
 */
public class AppendOnlyLogStore extends AbstractStore implements Closeable {

    static final int MAX_PUT_TASK_EXPIRY_SEC = 15;

    static final long MAX_PUT_WAIT_NANOS = TimeUnit.SECONDS.toNanos(2);

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStore.class.getSimpleName());
    private static final String ERR_IN_INIT = "An error occurred while initializing store: {0}";

    private final String id;

    private File store;

    private StoreWriter storeWriter;
    private StoreReader storeReader;

    private OutboxStore outboxStore;

    private final Map<String, Long> indexes;
    private long lastOffset;

    private volatile LogStoreRole role;
    private volatile int replicationTerm;
    private volatile boolean partitioned;
    private volatile AppendOnlyLogStore leader;

    private final Map<AppendOnlyLogStore, Boolean> followerStatusMap;
    private final ExecutorService replicatorService;

    private final Map<IdempotencyKey, AtomicReference<PutOperation>> idempotentOps;

    private final AtomicReference<CountDownLatch> replicationCallsWaitLatch;
    private final Object leaderReplicationMonitor;

    // Guards lockKey
    private final Object lockMonitor;

    // Guarded by lockMonitor
    private final Map<String, ReentrantLock> rwLocks;

    private final ExecutorService writerExecutor;
    private final ScheduledExecutorService writeTasksCleaner;

    private static String random8LetterHex() {
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, 7);
    }

    public AppendOnlyLogStore() throws StoreInitializationException {
        this(LogStoreRole.SINGLE);
    }

    public AppendOnlyLogStore(LogStoreRole role) throws StoreInitializationException {
        this(null, role);
    }

    public AppendOnlyLogStore(String path) throws StoreInitializationException {
        this(path, LogStoreRole.LEADER);
    }

    public AppendOnlyLogStore(String path, LogStoreRole storeRole) throws StoreInitializationException {
        try {
            initStore(path);

        } catch (Exception e) {
            throw new StoreInitializationException(
                    MessageFormat.format(ERR_IN_INIT, e.getMessage()), e);
        }

        id = "store_" + random8LetterHex();

        leaderReplicationMonitor = new Object();
        lockMonitor = new Object();

        seteRole(storeRole);

        followerStatusMap = new LinkedHashMap<>();
        replicatorService = Executors.newFixedThreadPool(5);
        replicationCallsWaitLatch = new AtomicReference<>(new CountDownLatch(0));

        idempotentOps = new ConcurrentHashMap<>();
        indexes = new ConcurrentHashMap<>();
        rwLocks = new ConcurrentHashMap<>();

        // Unbounded single thread that execute a write task
        writerExecutor = Executors.newSingleThreadExecutor();

        writeTasksCleaner = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread cleaner = new Thread(runnable, "write-tasks-cleaner");
            cleaner.setDaemon(true);

            return cleaner;
        });
        writeTasksCleaner.scheduleWithFixedDelay(() -> cleanExpiredIdempotentPutTasks(), MAX_PUT_TASK_EXPIRY_SEC,
                MAX_PUT_TASK_EXPIRY_SEC, TimeUnit.SECONDS);

        lastOffset = 0;
        buildIndexes();

        logger.info("Created store at: " + store.getAbsolutePath());
    }

    public String id() {
        return id;
    }

    @Override
    public void put(String key, String value) throws IOException {
        put(null, key, value);
    }

    @Override
    public long put(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        if (idempotencyKey != null && (idempotencyKey.key() == null || idempotencyKey.key().isBlank())) {
            throw new IllegalArgumentException("Idempotency key cannot be blank.");
        }

        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank.");
        }

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("value cannot be blank");
        }

        if (isFollower()) {
            return forwardToLeader(idempotencyKey, key, value);
        }

        checkFencingTerm();

        return writeRecord(Optional.ofNullable(idempotencyKey), key, value).offset();
    }

    @Override
    public String get(String key) throws IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank.");
        }

        if (!indexes.containsKey(key)) {
            return null;
        }

        Optional<DataRecord> record = storeReader.readLogRecordAt(indexes.get(key));

        return (record.isPresent() ? record.get().value() : null);
    }

    @Override
    public void clear() throws IOException {
        storeWriter.clear();
        indexes.clear();
    }

    @Override
    public void close() throws IOException {
        logger.info("Shutting down the store...");

        ExecutorService[] services = new ExecutorService[] { replicatorService, writerExecutor, writeTasksCleaner };
        Closeable[] closeables = new Closeable[] { storeReader, storeWriter, outboxStore };

        for (ExecutorService service : services) {
            service.shutdownNow();
        }

        try {
            writerExecutor.awaitTermination(MAX_PUT_WAIT_NANOS, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.error("An error occurred while trying to close the store", e);
            }
        }
    }

    public String getPath() {
        return store.getAbsolutePath();
    }

    public File getFile() {
        return new File(store.getPath());
    }

    public void becomeLeader() {
        seteRole(LogStoreRole.LEADER);
        leader = null;
    }

    public void registerFollower(AppendOnlyLogStore follower) {
        if (!followerStatusMap.containsKey(follower)) {
            followerStatusMap.put(follower, Boolean.TRUE);

            if (!follower.isFollower()) {
                follower.seteRole(LogStoreRole.FOLLOWER);
            }

            follower.setLeader(this);
        }
    }

    public void registerOutboxListener(Function<LogStoreEvent, Void> listener) {
        outboxStore.registerListener(listener);
    }

    public void publishOutboxEntries() {
        outboxStore.publishEvents();
    }

    public void lockKey(String key) {
        ReentrantLock keyLock = null;

        synchronized (lockMonitor) {
            keyLock = rwLocks.compute(key, (k, v) -> (v == null ? new ReentrantLock() : v));
        }

        keyLock.lock();
    }

    public void unlockKey(String key) {
        ReentrantLock lock = rwLocks.get(key);

        if (lock != null && lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    public void setAsLeader(int term) {
        replicationTerm = term;
        seteRole(LogStoreRole.LEADER);
    }

    void partitionFollower(AppendOnlyLogStore follower) {
        if (follower == null) {
            throw new IllegalArgumentException("Follower cannot be null.");
        }

        if (!followerStatusMap.containsKey(follower)) {
            throw new IllegalArgumentException("Not a registered follower.");
        }

        followerStatusMap.put(follower, Boolean.FALSE);
        follower.togglePartitioned();
    }

    void replicateRecord(int leaderTerm, DataRecord record) throws IOException {
        int latestTerm = ReplicationState.getCurrentTerm();

        if (latestTerm > leaderTerm) {
            logger.error("Current term, " + latestTerm + " greater than the received term " + leaderTerm);
            logger.error("Rejecting the replication request.");

            return;
        }

        writeRecord(Optional.empty(), record.key(), record.value());
    }

    void internalWaitTillReplicationsComplete() throws InterruptedException {
        replicationCallsWaitLatch.get().await(5, TimeUnit.SECONDS);
    }

    void internalExpireWriteTasks() {
        idempotentOps.forEach((k, v) -> {
            PutOperation existingOp = v.get();
            v.set(new PutOperation(existingOp.state(), existingOp.startedAt(),
                    System.nanoTime() - TimeUnit.SECONDS.toNanos(MAX_PUT_TASK_EXPIRY_SEC),
                    existingOp.entrySet()));
        });
    }

    Optional<LogStoreEntrySet> getLastEntry() throws IOException {
        return storeReader.readStoreEntryAt(lastOffset);
    }

    LogStoreEntrySet writeRecord(Optional<IdempotencyKey> optionalIdempotencyKey, String key, String value)
            throws IOException {

        IdempotencyKey idempotencyKey = (optionalIdempotencyKey.isEmpty() ? null : optionalIdempotencyKey.get());

        Future<LogStoreEntrySet> writeFuture = writerExecutor.submit(() -> writeData(idempotencyKey, key, value));

        try {
            return writeFuture.get(MAX_PUT_WAIT_NANOS, TimeUnit.NANOSECONDS);

        } catch (InterruptedException e) {
            logger.error("Store write task was interrupted, write operation could still be running in the background");
            writeFuture.cancel(true);

            Thread.currentThread().interrupt();

            throw new OperationStateUnknownException(idempotencyKey,
                    "Interrupted while waiting for the write to complete, status is unknown");

        } catch (TimeoutException e) {
            logger.error("Store write task timed out, write operation could still be running in the background");
            writeFuture.cancel(true);

            throw new OperationStateUnknownException(idempotencyKey,
                    "Timeout while awaiting write to complete, status is unknown");
        } catch (ExecutionException e) {
            Throwable exception = e.getCause();

            if (exception instanceof IOException io) {
                throw io;
            } else if (exception instanceof RuntimeException re) {
                throw re;
            }

            throw new IOException(e);
        }

    }

    LogStoreEntrySet writeData(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        AtomicReference<PutOperation> atomicOp = PutOperation.newAtomicOperation();

        if (idempotencyKey != null) {
            Optional<LogStoreEntrySet> entrySet = getIdempotentResult(idempotencyKey, key, value);

            if (entrySet.isPresent()) {
                return entrySet.get();
            }

            idempotentOps.put(idempotencyKey, atomicOp);
        }

        Throwable throwable = null;

        LogStoreEntrySet storeEntry = null;
        LogStoreEntrySet cdcEntry = null;

        try {
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("The writer thread was interrupted, backing off");
            }

            long timestamp = System.currentTimeMillis();

            storeEntry = insertDataRecord(key, value);
            cdcEntry = storeWriter.insertCdcRecord(CdcRecord.TYPE_INSERT,
                    timestamp, new LogStoreInsertEvent(key, value));

            lastOffset = storeEntry.offset();

            indexes.put(key, lastOffset);

            outboxStore.recordInserted(cdcEntry);

            if (isLeader()) {
                replicateRecordToFollowers((DataRecord) storeEntry.entry().record());
            }

            return storeEntry;

        } catch (Throwable e) {
            throwable = e;
            throw e;

        } finally {
            long startedAt = atomicOp.get().startedAt();
            boolean insertSuccessful = (storeEntry != null && cdcEntry != null);

            if (throwable == null || insertSuccessful) {
                atomicOp.set(
                        new PutOperation(PutOperationState.Successful, startedAt, System.nanoTime(), storeEntry));

            } else {
                atomicOp.set(new PutOperation(PutOperationState.Failed, startedAt, System.nanoTime(), null));
            }
        }
    }

    LogStoreEntrySet insertDataRecord(String key, String value) throws IOException {
        return storeWriter.insertRecord(key, value);
    }

    boolean holdsIdempotencyResult(IdempotencyKey key) {
        return idempotentOps.containsKey(key);
    }

    void cleanExpiredIdempotentPutTasks() {
        try {
            Set<String> cleanedUpKeys = new HashSet<>();

            idempotentOps.entrySet()
                    .removeIf(entry -> idempotentPutExpired(entry.getKey(), entry.getValue().get(), cleanedUpKeys));

            logger.debug("Cleaned up " + cleanedUpKeys.size() + " key(s)");
            if (!cleanedUpKeys.isEmpty() && logger.isTraceEnabled()) {
                logger.trace("Following keys were cleaned up: " + String.join(",", cleanedUpKeys));
            }
        } catch (RuntimeException e) {
            logger.error("An error occurred while cleaning expired write tasks", e);
        }
    }

    private boolean idempotentPutExpired(IdempotencyKey key, PutOperation putOp, Set<String> cleanedUpKeys) {
        if (putOp.state().equals(PutOperationState.Running)) {
            return false;
        }

        long deadline = putOp.completedAt() + TimeUnit.SECONDS.toNanos(MAX_PUT_TASK_EXPIRY_SEC);
        boolean expired = (deadline <= System.nanoTime());

        if (expired) {
            cleanedUpKeys.add(key.key());
        }

        return expired;
    }

    private long forwardToLeader(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        if (!partitioned) {
            return leader.put(idempotencyKey, key, value);
        }

        throw new IllegalStateException("ERR-PART: Unable to write value.");
    }

    private void checkFencingTerm() {
        if (isSingleNode()) {
            return;
        }

        int activeReplTerm = ReplicationState.getCurrentTerm();

        if (replicationTerm < activeReplTerm) {
            logger.error(MessageFormat.format("Active replication term, {0}, is ahead of my replication term, {1}.",
                    activeReplTerm, replicationTerm));
            logger.error("Cannot process write request!");

            throw new IllegalStateException();
        }
    }

    private void setLeader(AppendOnlyLogStore leader) {
        if (leader == null) {
            throw new IllegalArgumentException("Leader cannot be null.");
        }

        if (leader == this) {
            return;
        }

        this.leader = leader;
    }

    private void togglePartitioned() {
        partitioned = !partitioned;
    }

    private void initStore(String path) throws StoreInitializationException {
        try {

            store = (path == null ? File.createTempFile("wal-", ".db") : new File(path));

            storeWriter = new StoreWriter();
            storeReader = new StoreReader();

            outboxStore = new OutboxStore(storeReader, store);

        } catch (IOException fne) {
            throw new StoreInitializationException(fne);
        }
    }

    private void seteRole(LogStoreRole storeRole) {
        synchronized (leaderReplicationMonitor) {
            role = (storeRole == null ? LogStoreRole.FOLLOWER : storeRole);
            replicationTerm = (role == LogStoreRole.LEADER ? ReplicationState.getNextTerm() : 0);
        }
    }

    private void buildIndexes() {
        try {
            storeReader
                    .readCompletedLogEntries()
                    .stream()
                    .forEach(entrySet -> {
                        lastOffset = entrySet.offset();
                        indexes.put(((DataEntry) entrySet.entry()).record().key(), lastOffset);
                    });
        } catch (IOException e) {
            logger.error("Error whilst indexing: " + e.getMessage());
        }
    }

    private void replicateRecordToFollowers(DataRecord record) {
        if (!isLeader()) {
            throw new IllegalStateException();
        }

        synchronized (leaderReplicationMonitor) {
            replicationCallsWaitLatch.set(new CountDownLatch(followerStatusMap.size()));

            followerStatusMap.forEach((store, status) -> {
                replicatorService.submit(() -> {
                    try {
                        if (!status) {
                            return true;
                        }

                        store.replicateRecord(replicationTerm, record);

                        return true;
                    } finally {
                        replicationCallsWaitLatch.get().countDown();
                    }
                });
            });
        }
    }

    private boolean isSingleNode() {
        return LogStoreRole.SINGLE.equals(role);
    }

    private boolean isLeader() {
        return LogStoreRole.LEADER.equals(role);
    }

    private boolean isFollower() {
        return LogStoreRole.FOLLOWER.equals(role);
    }

    private Optional<LogStoreEntrySet> getIdempotentResult(IdempotencyKey idempotencyKey, String key, String value)
            throws PendingPutOperationException {

        AtomicReference<PutOperation> atomicOp = idempotentOps.get(idempotencyKey);
        if (atomicOp == null) {
            return Optional.empty();
        }

        PutOperation putOperation = atomicOp.get();

        if (putOperation.completed() && putOperation.successful()) {
            if (putOperation.hasRecord(key, value)) {
                return Optional.of(putOperation.entrySet());
            }

            throw new IllegalStateException("Given idempotency key is mapped to a different payload.");
        }

        return Optional.empty();
    }

    private enum PutOperationState {
        Running,
        Successful,
        Failed
    }

    private record PutOperation(PutOperationState state, long startedAt, long completedAt, LogStoreEntrySet entrySet) {

        public static AtomicReference<PutOperation> newAtomicOperation() {
            return new AtomicReference<>(new PutOperation(PutOperationState.Running, System.nanoTime(), -1, null));
        }

        public boolean successful() {
            return state == PutOperationState.Successful;
        }

        public boolean completed() {
            return switch (state) {
                case Successful, Failed -> true;
                default -> false;
            };
        }

        public boolean hasRecord(String key, String value) {
            return (entrySet != null && key.equals(dataRecord().key()) && value.equals(dataRecord().value()));
        }

        public DataRecord dataRecord() {
            return (DataRecord) entrySet.entry().record();
        }

    }

    public class StoreReader implements Closeable {

        private final LogStoreInput storeInput;

        public StoreReader() throws StoreInitializationException {
            try {
                storeInput = new LogStoreInput(store);

            } catch (FileNotFoundException fne) {
                throw new StoreInitializationException(fne);
            }
        }

        @Override
        public void close() throws IOException {
            storeInput.close();
        }

        public Optional<DataRecord> readLogRecordAt(long offset) throws IOException {
            Optional<LogStoreEntrySet> optionalEntry = storeInput.getEntryAt(offset);

            return (optionalEntry.isPresent() ? Optional.of(((DataEntry) optionalEntry.get().entry()).record())
                    : Optional.empty());
        }

        public Optional<CdcRecord> readCdcRecordAt(long offset) throws IOException {
            Optional<LogStoreEntrySet> optionalEntry = storeInput.getCdcEntryAt(offset);

            return (optionalEntry.isPresent() ? Optional.of(((CdcEntry) optionalEntry.get().entry()).record())
                    : Optional.empty());
        }

        Optional<LogStoreEntrySet> readStoreEntryAt(long offset) throws IOException {
            return storeInput.getEntryAt(offset);
        }

        List<LogStoreEntrySet> readCompletedLogEntries() throws IOException {
            return storeInput.getCompleteLogEntries();
        }

    }

    public class StoreWriter implements Closeable {

        private final LogStoreOuput storeOutput;

        public StoreWriter() throws StoreInitializationException {
            try {

                storeOutput = new LogStoreOuput(store);
                storeOutput.seek(storeOutput.length());

            } catch (IOException e) {
                throw new StoreInitializationException(e);
            }
        }

        @Override
        public void close() throws IOException {
            storeOutput.close();
        }

        public void clear() throws IOException {
            storeOutput.setLength(0);
        }

        public LogStoreEntrySet insertRecord(String key, String value) throws IOException {
            return insertRecord(LogStoreEntry.TYPE_DATA_ENTRY, new DataRecord(key, value));
        }

        public LogStoreEntrySet insertCdcRecord(byte type, long timestamp, Object event)
                throws IOException {
            return insertRecord(LogStoreEntry.TYPE_CDC_ENTRY, new CdcRecord(type, timestamp, event));
        }

        private LogStoreEntrySet insertRecord(byte type, LogStoreEntryPayload payload) throws IOException {
            long lastOffset = storeOutput.getFilePointer();
            LogStoreEntry storeEntry = storeOutput.putEntry(type, payload);

            return new LogStoreEntrySet(lastOffset, (storeOutput.pos() - lastOffset), storeEntry);
        }

    }

    public static void main(String[] args) {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            OperationBenchmark[] benchMarks = store.benchmarkOperations();

            logger.info("Write benchmarks: " + benchMarks[0]);
            logger.info("Read benchmarks: " + benchMarks[1]);
        } catch (StoreInitializationException | IOException e) {
            logger.error(e.getMessage());
        }
    }

}
