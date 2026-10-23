package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public final class LogStoreEntryOuput extends RandomLogStoreAccess {

    public static final int RECORD_OFFSET = (Byte.BYTES + Integer.BYTES + Long.BYTES);

    private final Checksum crc32Checksum;

    public LogStoreEntryOuput(File file) throws FileNotFoundException {
        super(file, "rwd");
        crc32Checksum = new CRC32();
    }

    public LogStoreEntry putEntry(byte type, String key, String value) throws IOException {
        LogStoreRecord record = new LogStoreRecord(key, value);

        String keyValuePair = record.toString();

        byte[] data = keyValuePair.getBytes();
        long checksum = getChecksum(data);

        write(ByteBuffer
                .allocate(RECORD_OFFSET + data.length)
                .put(type)
                .putInt(data.length)
                .putLong(checksum)
                .put(data)
                .array());
        write(LogStoreEntry.LOG_ENTRY_SEP.getBytes());

        return new LogStoreEntry(type, data.length, checksum, record);
    }

    private long getChecksum(byte[] contents) {
        crc32Checksum.reset();
        crc32Checksum.update(contents);

        return crc32Checksum.getValue();
    }

}
