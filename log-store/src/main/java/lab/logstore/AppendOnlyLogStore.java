package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
public class AppendOnlyLogStore extends AbstractStore implements Iterable<LogStoreEntrySet>, Closeable {

    private static final String ERR_IN_INIT = "An error occurred while initializing store: {0}";

    private File store;

    private AppendOnlyLogStoreWriter storeWriter;
    private AppendOnlyLogStoreReader storeReader;

    private final Map<String, Long> indexes;

    private volatile LogStoreRole role;
    private volatile int replicationTerm;
    private volatile boolean partitioned;
    private volatile AppendOnlyLogStore leader;

    private final Map<AppendOnlyLogStore, Boolean> followerStatusMap;
    private final ExecutorService replicatorService;

    private final Map<IdempotencyKey, Long> idempotentOps;

    static final int EOF = -1;

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

        seteRole(storeRole);

        followerStatusMap = new LinkedHashMap<>();
        replicatorService = Executors.newFixedThreadPool(5);

        idempotentOps = new HashMap<>();

        indexes = new LinkedHashMap<>();
        buildIndexes();

        System.out.println("Created store at: " + store.getAbsolutePath());
    }

    @Override
    public void write(String key, String value) throws IOException {
        if (isFollower()) {
            if (!partitioned) {
                leader.write(key, value);
                return;
            }

            throw new IllegalStateException("ERR-PART: Unable to write value.");
        }

        int activeReplTerm = ReplicationState.getCurrentTerm();

        if (replicationTerm < activeReplTerm) {
            System.err
                    .println(MessageFormat.format("Active replication term, {0}, is ahead of my replication term, {1}.",
                            activeReplTerm, replicationTerm));
            System.err.println("Cannot process write request!");

            throw new IllegalStateException();
        }

        writeRecord(key, value);
    }

    public long write(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        if (idempotentOps.containsKey(idempotencyKey)) {
            return idempotentOps.get(idempotencyKey);
        }

        write(key, value);

        long writeOffset = indexes.get(key);
        idempotentOps.put(idempotencyKey, writeOffset);

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

    @Override
    public Iterator<LogStoreEntrySet> iterator() {
        return storeReader.iterator();
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

    public void replicateRecord(int leaderTerm, LogStoreRecord record) throws IOException {
        int latestTerm = ReplicationState.getCurrentTerm();

        if (latestTerm > leaderTerm) {
            System.err.println("Current term, " + latestTerm + " greater than the received term " + leaderTerm);
            System.err.println("Rejecting the replication request.");

            return;
        }

        writeRecord(record.key(), record.value());
    }

    public void partitionFollower(AppendOnlyLogStore follower) {
        if (follower == null) {
            throw new IllegalArgumentException("Follower cannot be null.");
        }

        if (!followerStatusMap.containsKey(follower)) {
            throw new IllegalArgumentException("Not a registered follower.");
        }

        followerStatusMap.put(follower, Boolean.FALSE);
        follower.togglePartitioned();
    }

    Optional<LogStoreEntrySet> getLastEntry() throws IOException {
        Long[] offsets = indexes.values().toArray(new Long[0]);
        Arrays.sort(offsets);

        long offset = offsets[offsets.length - 1];

        return storeReader.readStoreEntryAt(offset);
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

    private void writeRecord(String key, String value) throws IOException {
        LogStoreEntrySet storeEntry = storeWriter.insertRecord(key, value);
        LogStoreEntrySet cdcEntry = storeWriter.insertCDCRecord(key, value);

        indexes.put(key, storeEntry.offset());

        if (isLeader()) {
            replicateRecordToFollowers(cdcEntry.entry().record());
        }
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
        role = (storeRole == null ? LogStoreRole.FOLLOWER : storeRole);
        replicationTerm = (role == LogStoreRole.LEADER ? ReplicationState.getNextTerm() : 0);
    }

    private void buildIndexes() {
        Iterator<LogStoreEntrySet> storeIterator = storeReader.iterator();

        while (storeIterator.hasNext()) {
            LogStoreEntrySet storeEntry = storeIterator.next();

            if (storeEntry != null) {
                indexes.put(storeEntry.entry().record().key(), storeEntry.offset());
            }
        }
    }

    private void replicateRecordToFollowers(LogStoreRecord record) {
        if (!isLeader()) {
            throw new IllegalStateException();
        }

        replicatorService.submit(() -> {
            followerStatusMap.entrySet().parallelStream().forEach((Entry<AppendOnlyLogStore, Boolean> followerSet) -> {
                if (!followerSet.getValue()) {
                    return;
                }

                try {
                    followerSet.getKey().replicateRecord(replicationTerm, record);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });
    }

    private boolean isLeader() {
        return LogStoreRole.LEADER.equals(role);
    }

    private boolean isFollower() {
        return LogStoreRole.FOLLOWER.equals(role);
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
