package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class LogStoreEntryOuput extends RandomLogStoreAccess {

    public static final int RECORD_OFFSET = (Byte.BYTES + Integer.BYTES + Long.BYTES);

    public LogStoreEntryOuput(File file) throws FileNotFoundException {
        super(file, "rwd");
    }

    public LogStoreEntry putEntry(byte type, String key, String value) throws IOException {
        LogStoreRecord record = new LogStoreRecord(key, value);

        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = value.getBytes();

        byte[] data = ByteBuffer
            .allocate(Integer.BYTES + keyBytes.length + Integer.BYTES + valueBytes.length)
            .putInt(keyBytes.length)
            .put(keyBytes)
            .putInt(valueBytes.length)
            .put(valueBytes)
            .array();

        long checksum = getChecksum(data);

        write(ByteBuffer
                .allocate(RECORD_OFFSET + data.length)
                .put(type)
                .putInt(data.length)
                .putLong(checksum)
                .put(data)
                .array());

        return new LogStoreEntry(type, data.length, checksum, record);
    }

}
