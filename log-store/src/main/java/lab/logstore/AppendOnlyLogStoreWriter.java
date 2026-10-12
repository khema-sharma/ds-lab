package lab.logstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class AppendOnlyLogStoreWriter implements Closeable {
    
    private FileWriter writer;

    private final Checksum crc32Checksum;
    private final File store;

    public AppendOnlyLogStoreWriter(File store) throws StoreInitializationException {
        if (store == null) {
            throw new IllegalArgumentException("store cannot be null.");
        }
    
        this.store = store;
        crc32Checksum = new CRC32();

        try {
            writer = initWriter();
        
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

    public void insertRecord(String key, String value) throws IOException {
        insertRecord(new LogStoreRecord(key, value));
    }

    public void insertRecord(LogStoreRecord record) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("record cannot be null.");
        }

        crc32Checksum.reset();
        crc32Checksum.update(record.getBytes());

        int length = record.getBytes().length;
        long checksum = crc32Checksum.getValue();

        writer.write(new LogStoreEntry(-1, length, checksum, record).toString() + AppendOnlyLogStoreReader.RECORD_SEP);
        writer.flush();
    }

    private FileWriter initWriter() throws IOException {
        return new FileWriter(store, Charset.forName("utf-8"), true);
    }

}
