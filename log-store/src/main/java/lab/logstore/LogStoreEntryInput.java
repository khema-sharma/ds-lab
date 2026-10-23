package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class LogStoreEntryInput extends RandomLogStoreAccess {
    private static final int EOF = -1;

    private final Checksum crc32Checksum;

    public LogStoreEntryInput(File file) throws FileNotFoundException {
        super(file, "r");
        crc32Checksum = new CRC32();
    }

    public Optional<LogStoreEntrySet> getEntryAt(long offset) throws IOException {
        if (offset < 0 || offset > length()) {
            throw new IllegalArgumentException("offset must be between 0 and length of the store.");
        }

        seek(offset);

        return getNextEntry();
    }

    public Optional<LogStoreEntrySet> getNextEntry() throws IOException {
        long entryOffset = pos();

        try {

            byte type = (byte) readByte();

            if (!LogStoreEntry.isValidEntryType(type)) {
                throw new IllegalStateException("Invalid entry type.");
            }

            int length = readInt();
            long expectedChecksum = readLong();
            byte[] recordBytes = new byte[length];

            readFully(recordBytes);

            long actualChecksum = getChecksum(recordBytes);
            if (expectedChecksum != actualChecksum) {
                throw new IllegalStateException("Invalid record, checksum mismatch.");
            }

            Optional<LogStoreRecord> record = LogStoreRecord.fromBytes(recordBytes);
            if (record.isEmpty()) {
                throw new IllegalStateException("Could not construct record from its bytes.");
            }

            long entryLength = (pos() - entryOffset);

            // Cross linefeed
            read();

            return Optional.of(new LogStoreEntrySet(entryOffset, entryLength,
                    new LogStoreEntry(type, length, actualChecksum, record.get())));

        } catch (IllegalStateException ise) {
            seekTillLineFeed();
            return Optional.empty();
        }
    }

    private long getChecksum(byte[] contents) {
        crc32Checksum.reset();
        crc32Checksum.update(contents);

        return crc32Checksum.getValue();
    }

    private long seekTillLineFeed() throws IOException {
        long bytesRead = 0;

        int nByte = read();
        if (nByte != EOF) {
            bytesRead++;
        }

        while (nByte != EOF && !Character.toString(nByte).equals(LogStoreEntry.LOG_ENTRY_SEP)) {
            nByte = read();
            if (nByte != EOF) {
                bytesRead++;
            }
        }

        return bytesRead;
    }

}
