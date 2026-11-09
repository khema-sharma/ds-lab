package lab.ds.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStore.class);

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

    private final Map<IdempotencyKey, IdempotentDataRecord> idempotentOps;

    private final AtomicReference<CountDownLatch> replicationCallsWaitLatch;
    private final Object leaderReplicationMonitor;

    // Guards lockKey
    private final Object lockMonitor;

    // Guarded by lockMonitor
    private final Map<String, ReentrantLock> rwLocks;

    private final ExecutorService writerExecutor;

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

        lastOffset = 0;
        buildIndexes();

        logger.info("Created store at: " + store.getAbsolutePath());
    }

    public String id() {
        return id;
    }

    @Override
    public void put(String key, String value) throws IOException {
        if (forwardToLeader(key, value)) {
            return;
        }

        checkFencingTerm();

        writeRecord(key, value);
    }

    @Override
    public long put(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        if (idempotentOps.containsKey(idempotencyKey)) {
            IdempotentDataRecord recordOffset = idempotentOps.get(idempotencyKey);

            if (recordOffset.record().key().equals(key) && recordOffset.record().value().equals(value)) {
                return ((IdempotentDataRecord) idempotentOps.get(idempotencyKey)).timestamp();
            }

            throw new IllegalStateException("Given idempotency key is mapped to a different payload.");
        }

        put(key, value);

        long writeOffset = indexes.get(key);

        idempotentOps.put(idempotencyKey, new IdempotentDataRecord(writeOffset, new DataRecord(key, value)));

        return writeOffset;
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
        storeWriter.close();
        storeReader.close();
        outboxStore.close();

        replicatorService.shutdownNow();
        writerExecutor.shutdownNow();
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

        writeRecord(record.key(), record.value());
    }

    void internalWaitTillReplicationsComplete() throws InterruptedException {
        replicationCallsWaitLatch.get().await(5, TimeUnit.SECONDS);
    }

    Optional<LogStoreEntrySet> getLastEntry() throws IOException {
        return storeReader.readStoreEntryAt(lastOffset);
    }

    private boolean forwardToLeader(String key, String value) throws IOException {
        if (!isFollower()) {
            return false;
        }

        if (!partitioned) {
            leader.put(key, value);
            return true;
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

    private long writeRecord(String key, String value) throws IOException {
        try {
            ExecutorCompletionService<Long> putCompletionService = new ExecutorCompletionService<>(writerExecutor);

            putCompletionService.submit(() -> {
                long timestamp = System.currentTimeMillis();

                LogStoreEntrySet storeEntry = storeWriter.insertRecord(key, value);
                LogStoreEntrySet cdcEntry = storeWriter.insertCdcRecord(CdcRecord.TYPE_INSERT,
                        timestamp, new LogStoreInsertEvent(key, value));

                lastOffset = storeEntry.offset();

                indexes.put(key, lastOffset);

                outboxStore.recordInserted(cdcEntry);

                if (isLeader()) {
                    replicateRecordToFollowers(((DataEntry) storeEntry.entry()).record());
                }

                return lastOffset;
            });

            Future<Long> writeResult = putCompletionService.poll(3, TimeUnit.SECONDS);

            if (writeResult == null) {
                throw new OperationStateUnknownException(null,
                        "Timeout while awaiting for the write to complete, status is unknown");
            }

            return writeResult.get();

        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new OperationStateUnknownException(null,
                    "Interrupted while awaiting for the write to complete, status is unknown");

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

    private record IdempotentDataRecord(long timestamp, DataRecord record) {
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
