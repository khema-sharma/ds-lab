package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class AppendOnlyLogStoreWriter implements Closeable {

    private FileWriter writer;

    private final Checksum crc32Checksum;
    private final File store;

    private long pos;

    public AppendOnlyLogStoreWriter(File store) throws StoreInitializationException {
        if (store == null) {
            throw new IllegalArgumentException("store cannot be null.");
        }

        this.store = store;
        crc32Checksum = new CRC32();

        try {
            writer = initWriter();
            pos = Files.size(store.toPath());

        } catch (IOException e) {
            throw new StoreInitializationException(e);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    public void clear() throws IOException {
        writer = initWriter();

        writer.write("");
        writer.flush();
    }

    public LogStoreEntry insertRecord(String key, String value) throws IOException {
        return insertRecord(new LogStoreRecord(key, value));
    }

    public LogStoreEntry insertRecord(LogStoreRecord record) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("record cannot be null.");
        }

        crc32Checksum.reset();
        crc32Checksum.update(record.getBytes());

        long entryPos = pos;

        int length = record.getBytes().length;
        long checksum = crc32Checksum.getValue();

        LogStoreEntry storeEntry = new LogStoreEntry(entryPos, length, checksum, record);

        String storeEntryString = storeEntry.toString() + AppendOnlyLogStoreReader.RECORD_SEP;

        writer.write(storeEntryString);
        writer.flush();

        pos += storeEntryString.length();

        return storeEntry;
    }

    private FileWriter initWriter() throws IOException {
        return new FileWriter(store, Charset.forName("utf-8"), true);
    }

}
