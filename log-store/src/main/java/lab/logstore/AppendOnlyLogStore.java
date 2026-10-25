package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
import java.util.concurrent.ScheduledExecutorService;
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

        idempotentOps.put(idempotencyKey, new LogRecordOffset(writeOffset, new DataRecord(key, value)));

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

    public void registerOutboxListener(Function<LogStoreEvent, Void> listener) {
        outboxStore.registerListener(listener);
    }

    public void publishOutboxEntries() {
        outboxStore.publishEvents();
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
        long timestamp = System.currentTimeMillis();

        LogStoreEntrySet storeEntry = storeWriter.insertRecord(key, value);
        LogStoreEntrySet cdcEntry = storeWriter.insertCDCRecord(CdcRecord.TYPE_INSERT,
                timestamp, new LogStoreInsertEvent(key, value));

        lastOffset = storeEntry.offset();

        indexes.put(key, lastOffset);

        outboxStore.recordInserted(cdcEntry);

        if (isLeader()) {
            replicateRecordToFollowers(((DataEntry) storeEntry.entry()).record());
        }

        return lastOffset;
    }

    private void initStore(String path) throws StoreInitializationException {
        try {

            store = (path == null ? File.createTempFile("wal-", ".db") : new File(path));

            storeWriter = new StoreWriter();
            storeReader = new StoreReader();

            outboxStore = new OutboxStore(store);

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
            System.err.println("Error whilst indexing: " + e.getMessage());
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

    private boolean isLeader() {
        return LogStoreRole.LEADER.equals(role);
    }

    private boolean isFollower() {
        return LogStoreRole.FOLLOWER.equals(role);
    }

    private record LogRecordOffset(long offset, DataRecord record) {
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

    public class StoreReader implements Closeable {

        private final LogStoreEntryInput storeInput;

        public StoreReader() throws StoreInitializationException {
            try {
                storeInput = new LogStoreEntryInput(store);

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

        public Optional<CdcRecord> readCDCRecordAt(long offset) throws IOException {
            Optional<LogStoreEntrySet> optionalEntry = storeInput.getCDCEntryAt(offset);

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

    private class StoreWriter implements Closeable {

        private final LogStoreEntryOuput storeOutput;

        public StoreWriter() throws StoreInitializationException {
            try {

                storeOutput = new LogStoreEntryOuput(store);
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

        public LogStoreEntrySet insertCDCRecord(byte type, long timestamp, Object event)
                throws IOException {
            return insertRecord(LogStoreEntry.TYPE_CDC_ENTRY, new CdcRecord(type, timestamp, event));
        }

        private LogStoreEntrySet insertRecord(byte type, LogStoreEntryPayload payload) throws IOException {
            long lastOffset = storeOutput.getFilePointer();
            LogStoreEntry storeEntry = storeOutput.putEntry(type, payload);

            return new LogStoreEntrySet(lastOffset, (storeOutput.pos() - lastOffset), storeEntry);
        }

    }

    private final class OutboxStore {

        private final Path outboxPath;
        private final Path tmpOutboxPath;

        private final List<Function<LogStoreEvent, Void>> outboxListeners;

        private volatile List<UnpublishedOutboxEntry> unpublishedEntries;

        private final Object stateMonitor;
        private final ScheduledExecutorService backupExecutorService;

        public OutboxStore(File logStore) throws IOException {
            if (logStore == null || !logStore.isFile()) {
                throw new IllegalArgumentException("Invalid store file. outboxStore must be a file.");
            }

            String logStoreName = logStore.getName();

            logStoreName = logStoreName.replace("wal-", "");
            logStoreName = logStoreName.replace(".db", "");

            String outboxFileName = "outbox-" + logStoreName;

            outboxPath = Paths.get(logStore.getParent(), outboxFileName + ".bin");
            tmpOutboxPath = Paths.get(logStore.getParent(), outboxFileName + ".tmp");

            unpublishedEntries = new LinkedList<>();
            outboxListeners = new LinkedList<>();

            stateMonitor = new Object();

            backupExecutorService = Executors.newScheduledThreadPool(1);
            backupExecutorService.scheduleWithFixedDelay(new OutboxBackupThread(), 0, 3, TimeUnit.SECONDS);

            loadState();
        }

        public void recordInserted(LogStoreEntrySet cdcRecordSet) {
            synchronized (stateMonitor) {
                unpublishedEntries
                        .add(new UnpublishedOutboxEntry(((CdcEntry) cdcRecordSet.entry()).record().timestamp(),
                                cdcRecordSet.offset()));
            }
        }

        public void registerListener(Function<LogStoreEvent, Void> listener) {
            if (listener == null) {
                throw new IllegalArgumentException("listener cannot be null.");
            }

            outboxListeners.add(listener);
        }

        public void publishEvents() {
            synchronized (stateMonitor) {
                notifyListeners();
            }
        }

        private void loadState() throws IOException {
            if (!Files.exists(outboxPath) || Files.size(outboxPath) == 0) {
                return;
            }

            try (RandomAccessFile outbox = new RandomAccessFile(outboxPath.toFile(), "r")) {
                int size = outbox.readInt();

                if (size <= 0) {
                    return;
                }

                List<UnpublishedOutboxEntry> entries = new LinkedList<>();
                for (int i = 0; i < size; i++) {
                    entries.add(UnpublishedOutboxEntry.createFromInput(outbox));
                }

                synchronized (stateMonitor) {
                    unpublishedEntries = entries;
                }
            }
        }

        private void notifyListeners() {
            unpublishedEntries
                    .stream()
                    .forEach(entry -> {
                        outboxListeners.forEach(listener -> {
                            long eventId = entry.id();
                            long offset = entry.offset();

                            Optional<CdcRecord> cdcRecord = Optional.empty();

                            try {
                                cdcRecord = storeReader.readCDCRecordAt(offset);
                            } catch (IOException e) {
                                System.err.println("Error while fetching CDC entry for log at offset " +
                                        offset + ": "
                                        + e.getMessage());
                            }

                            if (cdcRecord.isPresent() && cdcRecord.get().timestamp() == eventId) {
                                try {
                                    listener.apply((LogStoreEvent) cdcRecord.get().event());
                                } catch (Exception e) {
                                    System.err
                                            .println("Ignoring the error that occurred whilst calling outbox listener: "
                                                    + e.getMessage());
                                }
                            }
                        });
                    });
            unpublishedEntries.clear();
        }

        private class OutboxBackupThread extends Thread {

            private OutboxBackupThread() {
                super();
                setDaemon(true);
            }

            @Override
            public void run() {
                synchronized (stateMonitor) {
                    try (OutputStream stream = Files.newOutputStream(tmpOutboxPath, StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

                        if (!unpublishedEntries.isEmpty()) {
                            int totalBytes = Integer.BYTES
                                    + (unpublishedEntries.size() * (UnpublishedOutboxEntry.BYTES));

                            ByteBuffer outboxBuffer = ByteBuffer.allocate(totalBytes);

                            outboxBuffer.putInt(unpublishedEntries.size());

                            unpublishedEntries.forEach(entry -> entry.writeEntry(outboxBuffer));

                            stream.write(outboxBuffer.array());
                            stream.flush();
                        }

                    } catch (IOException e) {
                        System.err.println("An error occurred whilst save temporary outbox: " + e.getMessage());
                    }

                    try {
                        Files.move(tmpOutboxPath, outboxPath, StandardCopyOption.ATOMIC_MOVE,
                                StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        System.err.println("An error occurred whilst saving outbox: " + e.getMessage());
                    }
                }
            }

        }

    }

}