package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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
 * <li>The {@link #write(String, String)} method writes the given {@code(key,
 * value)} pair to the store
 * <li>The {@link #read(String)} method return the value of the given
 * {@code key} from the store
 * </ul>
 */
public class AppendOnlyLogStore extends AbstractStore implements Closeable {

    private static final String ERR_IN_INIT = "An error occurred while initializing store: {0}";

    private File store;

    private AppendOnlyLogStoreWriter storeWriter;
    private AppendOnlyLogStoreReader storeReader;

    private final Map<String, Long> indexes;
    private long lastOffset;

    private final Map<String, Boolean> outboxEntries;
    private final List<Function<LogStoreRecord, Void>> outboxListeners;

    private volatile LogStoreRole role;
    private volatile int replicationTerm;
    private volatile boolean partitioned;
    private volatile AppendOnlyLogStore leader;

    private final Map<AppendOnlyLogStore, Boolean> followerStatusMap;
    private final ExecutorService replicatorService;

    private final Map<IdempotencyKey, LogRecordOffset> idempotentOps;

    private final AtomicReference<CountDownLatch> replicationCallsWaitLatch;
    private final Object leaderReplicationMonitor;

    public AppendOnlyLogStore() throws StoreInitializationException {
        this((String) null);
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

        leaderReplicationMonitor = new Object();

        seteRole(storeRole);

        followerStatusMap = new LinkedHashMap<>();
        replicatorService = Executors.newFixedThreadPool(5);
        replicationCallsWaitLatch = new AtomicReference<>(new CountDownLatch(0));

        idempotentOps = new ConcurrentHashMap<>();
        indexes = new ConcurrentHashMap<>();
        outboxEntries = new ConcurrentHashMap<>();
        outboxListeners = new LinkedList<>();

        lastOffset = 0;
        buildIndexes();

        System.out.println("Created store at: " + store.getAbsolutePath());
    }

    @Override
    public void write(String key, String value) throws IOException {
        if (forwardToLeader(key, value)) {
            return;
        }

        checkFencingTerm();
        writeRecord(key, value);
    }

    public long write(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        if (idempotentOps.containsKey(idempotencyKey)) {
            LogRecordOffset recordOffset = idempotentOps.get(idempotencyKey);

            if (recordOffset.record().key().equals(key) && recordOffset.record().value().equals(value)) {
                return ((LogRecordOffset) idempotentOps.get(idempotencyKey)).offset();
            }

            throw new IllegalStateException("Given idempotency key is mapped to a different payload.");
        }

        write(key, value);

        long writeOffset = indexes.get(key);

        idempotentOps.put(idempotencyKey, new LogRecordOffset(writeOffset, new LogStoreRecord(key, value)));

        return writeOffset;
    }

    @Override
    public String read(String key) throws IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank.");
        }

        if (!indexes.containsKey(key)) {
            return null;
        }

        Optional<LogStoreRecord> record = storeReader.readLogRecordAt(indexes.get(key));

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

        replicatorService.shutdownNow();
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
            follower.setLeader(this);
        }
    }

    public void registerOutboxListener(Function<LogStoreRecord, Void> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener cannot be null.");
        }

        outboxListeners.add(listener);
    }

    public void publishOutboxEntries() {
        outboxEntries
                .entrySet()
                .stream()
                .filter(entry -> !entry.getValue())
                .forEach(entry -> {
                    outboxListeners.forEach(listener -> {
                        String key = entry.getKey();
                        Long offset = indexes.get(key);

                        if (offset == null) {
                            // Ignoring for now
                            return;
                        }

                        try {
                            Optional<LogStoreRecord> cdcRecord = storeReader.readCDCRecordForEntryAt(offset);

                            if (cdcRecord.isPresent()) {
                                listener.apply(cdcRecord.get());
                                outboxEntries.put(key, true);
                            }
                        } catch (IOException e) {
                            System.err.println("Error while fetching CDC entry for log at offset " + offset + ": "
                                    + e.getMessage());
                        }
                    });
                });
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

    void replicateRecord(int leaderTerm, LogStoreRecord record) throws IOException {
        int latestTerm = ReplicationState.getCurrentTerm();

        if (latestTerm > leaderTerm) {
            System.err.println("Current term, " + latestTerm + " greater than the received term " + leaderTerm);
            System.err.println("Rejecting the replication request.");

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
            leader.write(key, value);
            return true;
        }

        throw new IllegalStateException("ERR-PART: Unable to write value.");
    }

    private void checkFencingTerm() {
        int activeReplTerm = ReplicationState.getCurrentTerm();

        if (replicationTerm < activeReplTerm) {
            System.err
                    .println(MessageFormat.format("Active replication term, {0}, is ahead of my replication term, {1}.",
                            activeReplTerm, replicationTerm));
            System.err.println("Cannot process write request!");

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
        LogStoreEntrySet storeEntry = storeWriter.insertRecord(key, value);
        LogStoreEntrySet cdcEntry = storeWriter.insertCDCRecord(key, value);

        lastOffset = storeEntry.offset();

        indexes.put(key, lastOffset);
        outboxEntries.put(key, false);

        if (isLeader()) {
            replicateRecordToFollowers(cdcEntry.entry().record());
        }

        return lastOffset;
    }

    private void initStore(String path) throws StoreInitializationException {
        try {
            store = (path == null ? File.createTempFile("write-ahead", ".db") : new File(path));

            storeWriter = new AppendOnlyLogStoreWriter(store);
            storeReader = new AppendOnlyLogStoreReader(store);
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
                        indexes.put(entrySet.entry().record().key(), lastOffset);
                        outboxEntries.put(entrySet.entry().record().key(), false);
                    });
        } catch (IOException e) {
            System.err.println("Error whilst indexing: " + e.getMessage());
        }
    }

    private void replicateRecordToFollowers(LogStoreRecord record) {
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

    private boolean isLeader() {
        return LogStoreRole.LEADER.equals(role);
    }

    private boolean isFollower() {
        return LogStoreRole.FOLLOWER.equals(role);
    }

    private record LogRecordOffset(long offset, LogStoreRecord record) {
    }

    public static void main(String[] args) {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            OperationBenchmark[] benchMarks = store.benchmarkOperations();

            System.out.println("Write benchmarks: " + benchMarks[0]);
            System.out.println("Read benchmarks: " + benchMarks[1]);
        } catch (StoreInitializationException | IOException e) {
            e.printStackTrace();
        }
    }

}
