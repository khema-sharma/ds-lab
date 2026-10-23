package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class AppendOnlyLogStoreReader implements Closeable {
    private final LogStoreEntryInput storeInput;

    public AppendOnlyLogStoreReader(File store) throws StoreInitializationException {
        if (store == null) {
            throw new IllegalArgumentException("store cannot be null.");
        }

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

    public Optional<LogStoreRecord> readLogRecordAt(long offset) throws IOException {
        Optional<LogStoreEntrySet> optionalEntry = readStoreEntryAt(offset);

        return (!optionalEntry.isEmpty() ? Optional.of(optionalEntry.get().entry().record()) : Optional.empty());
    }

    public Optional<LogStoreRecord> readCDCRecordForEntryAt(long offset) throws IOException {
        Optional<LogStoreEntrySet> entry = readStoreEntryAt(offset);

        if (entry.isPresent() && entry.get().entry().isLogEntry()) {
            entry = storeInput.getNextEntry();

            if (entry.isPresent() && entry.get().entry().isCDCEntry()) {
                return Optional.of(entry.get().entry().record());
            }
        }

        return Optional.empty();
    }

    Optional<LogStoreEntrySet> readStoreEntryAt(long offset) throws IOException {
        return storeInput.getEntryAt(offset);
    }

    List<LogStoreEntrySet> readCompletedLogEntries() throws IOException {
        return storeInput.getCompletedLogEntries();    
    }

}
