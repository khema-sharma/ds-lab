package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

public class AppendOnlyLogStoreReader implements Closeable {
    final File logStore;
    private final LogStoreEntryInput storeInput;

    public AppendOnlyLogStoreReader(File store) throws StoreInitializationException {
        if (store == null) {
            throw new IllegalArgumentException("store cannot be null.");
        }

        logStore = store;

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

    public Iterator<LogStoreEntrySet> iterator() {
        return new AppendOnlyLogStoreIterator(logStore);
    }

    public Optional<LogStoreRecord> readLogRecordAt(long offset) throws IOException {
        Optional<LogStoreEntrySet> optionalEntry = readStoreEntryAt(offset);

        return (!optionalEntry.isEmpty() ? Optional.of(optionalEntry.get().entry().record()) : Optional.empty());
    }

    Optional<LogStoreEntrySet> readStoreEntryAt(long offset) throws IOException {
        return storeInput.getEntryAt(offset);
    }

}
