package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class AppendOnlyLogStoreWriter implements Closeable {

    private final LogStoreEntryOuput storeOutput;
    private final File store;

    public AppendOnlyLogStoreWriter(File store) throws StoreInitializationException {
        if (store == null) {
            throw new IllegalArgumentException("store cannot be null.");
        }

        this.store = new File(store.getAbsolutePath());

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
        return insertRecord(LogStoreEntry.TYPE_LOG_ENTRY, key, value);
    }

    public LogStoreEntrySet insertCDCRecord(String key, String value) throws IOException {
        return insertRecord(LogStoreEntry.TYPE_CDC_ENTRY, key, value);
    }

    public File getStore() {
        return store;
    }

    private LogStoreEntrySet insertRecord(byte type, String key, String value) throws IOException {
        long lastOffset = storeOutput.getFilePointer();
        LogStoreEntry storeEntry = storeOutput.putEntry(type, key, value);

        return new LogStoreEntrySet(lastOffset, (storeOutput.pos() - lastOffset), storeEntry);
    }

}
