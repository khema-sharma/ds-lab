package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Optional;

public class AppendOnlyLogStore extends AbstractStore implements Iterable<LogStoreEntry>, Closeable {

    private static final String ERR_IN_INIT = "An error occurred while initializing store: {0}";

    private File store;

    private AppendOnlyLogStoreWriter storeWriter;
    private AppendOnlyLogStoreReader storeReader;

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

        System.out.println("Created store at: " + store.getAbsolutePath());
    }

    @Override
    public void write(String key, String value) throws IOException {
        storeWriter.insertRecord(key, value);
    }

    @Override
    public String read(String key) throws IOException {
        Optional<LogStoreRecord> record = storeReader.readRecord(key);

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
        return storeReader.new LogStoreIterator();
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
