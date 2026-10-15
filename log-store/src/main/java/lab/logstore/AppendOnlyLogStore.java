package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
public class AppendOnlyLogStore extends AbstractStore implements Iterable<LogStoreEntry>, Closeable {

    private static final String ERR_IN_INIT = "An error occurred while initializing store: {0}";

    private File store;

    private AppendOnlyLogStoreWriter storeWriter;
    private AppendOnlyLogStoreReader storeReader;

    private final LogStoreRole role;
    private final Map<String, Long> indexes;

    private AppendOnlyLogStore electedLeader;
    
    private final List<AppendOnlyLogStore> followers;
    private final ExecutorService replicatorService;

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

        role = (storeRole == null ? LogStoreRole.FOLLOWER : storeRole);
        indexes = new LinkedHashMap<>();
        followers = new LinkedList<>();
        replicatorService = Executors.newSingleThreadExecutor();

        buildIndexes();

        System.out.println("Created store at: " + store.getAbsolutePath());
    }

    @Override
    public void write(String key, String value) throws IOException {
        LogStoreEntry storeEntry = storeWriter.insertRecord(key, value);
        indexes.put(key, storeEntry.offset());

        if (isLeader()) {
            replicateRecordToFollowers(storeEntry.record());
        }
    }

    @Override
    public String read(String key) throws IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank.");
        }

        if (!indexes.containsKey(key)) {
            return null;
        }

        Optional<LogStoreRecord> record = storeReader.readRecordAt(indexes.get(key));

        return (record.isPresent() ? record.get().value() : null);
    }

    @Override
    public void clear() throws IOException {
        storeWriter.clear();
    }

    @Override
    public void close() throws IOException {
        storeWriter.close();
        storeReader.close();
    }

    @Override
    public Iterator<LogStoreEntry> iterator() {
        return storeReader.iterator();
    }

    public String getPath() {
        return store.getAbsolutePath();
    }

    public void registerFollower(AppendOnlyLogStore follower) {
        if (!followers.contains(follower)) {
            follower.setLeader(this);
            followers.add(follower);
        }
    }

    public void replicateRecord(LogStoreRecord record) throws IOException {
        write(record.key(), record.value());
    }

    public void setLeader(AppendOnlyLogStore store) {
        if (store == null) {
            throw new IllegalArgumentException("Leader cannot be null.");
        }

        if (isFollower()) {
            electedLeader = store;
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

    private void buildIndexes() {
        Iterator<LogStoreEntry> storeIterator = storeReader.iterator();

        while (storeIterator.hasNext()) {
            LogStoreEntry storeEntry = storeIterator.next();

            if (storeEntry != null) {
                indexes.put(storeEntry.record().key(), storeEntry.offset());
            }
        }
    }

    private void replicateRecordToFollowers(LogStoreRecord record) {
        if (!isLeader()) {
            throw new IllegalStateException();
        }

        for (AppendOnlyLogStore follower : followers) {
            replicatorService.submit(() -> {
                try {
                    follower.replicateRecord(record);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
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
