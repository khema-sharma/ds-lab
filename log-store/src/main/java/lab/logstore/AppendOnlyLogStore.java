package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class AppendOnlyLogStore extends AbstractStore implements Iterable<LogStoreEntry>, Closeable {

    private static final String ERR_IN_INIT = "An error occurred while initializing store: {0}";

    private File store;

    private AppendOnlyLogStoreWriter storeWriter;
    private AppendOnlyLogStoreReader storeReader;

    private final Map<String, Long> indexes;

    public AppendOnlyLogStore() throws StoreInitializationException {
        this(null);
    }

    public AppendOnlyLogStore(String path) throws StoreInitializationException {
        try {
            initStore(path);

        } catch (Exception e) {
            throw new StoreInitializationException(
                    MessageFormat.format(ERR_IN_INIT, e.getMessage()), e);
        }
        
        indexes = new HashMap<>();

        buildIndexes();

        System.out.println("Created store at: " + store.getAbsolutePath());
    }

    @Override
    public void write(String key, String value) throws IOException {
        LogStoreEntry storeEntry = storeWriter.insertRecord(key, value);
        indexes.put(key, storeEntry.offset());
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
        return storeReader.new AppendOnlyLogStoreIterator();
    }

    public String getPath() {
        return store.getAbsolutePath();
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
        Iterator<LogStoreEntry> storeIterator = storeReader.new AppendOnlyLogStoreIterator();

        while (storeIterator.hasNext()) {
            LogStoreEntry storeEntry = storeIterator.next();

            if (storeEntry != null) {
                indexes.put(storeEntry.record().key(), storeEntry.offset());
            }
        }
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
