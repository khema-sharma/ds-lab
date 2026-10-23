package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class LogStoreEntryInput extends RandomLogStoreAccess {

    private final Object readMonitor;

    public LogStoreEntryInput(File file) throws FileNotFoundException {
        super(file, "r");

        readMonitor = new Object();
    }

    public Optional<LogStoreEntrySet> getEntryAt(long offset) throws IOException {
        if (offset < 0 || offset > length()) {
            throw new IllegalArgumentException("offset must be between 0 and length of the store.");
        }

        synchronized (readMonitor) {
            seek(offset);
            return getNextEntry();
        }
    }

    public List<LogStoreEntrySet> getCompletedLogEntries() throws IOException {
        long lastPos = pos();
        List<LogStoreEntrySet> allRecordEntries = new LinkedList<>();

        while (!isEOF()) {
            long previousPos = pos();

            try {
                Optional<LogStoreEntrySet> recordEntrySet = getNextEntry();
                
                if (recordEntrySet.isPresent()) {
                    if (!recordEntrySet.get().entry().isLogEntry()) {
                        break;
                    }

                    Optional<LogStoreEntrySet> cdcEntrySet = getNextEntry();

                    if (cdcEntrySet.isPresent()) {
                        if (!cdcEntrySet.get().entry().isCDCEntry()) {
                            break;
                        }

                        allRecordEntries.add(recordEntrySet.get());
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading an entry at offset: " + previousPos);
                break;
            }
        }

        seek(lastPos);

        return allRecordEntries;
    }

    Optional<LogStoreEntrySet> getNextEntry() throws IOException {
        long entryOffset = pos();

        synchronized (readMonitor) {
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

                Optional<LogStoreRecord> record = readLogStoreRecord(recordBytes);
                if (record.isEmpty()) {
                    throw new IllegalStateException("Could not parse record from the stream.");
                }

                long entryLength = (pos() - entryOffset);

                return Optional.of(new LogStoreEntrySet(entryOffset, entryLength,
                        new LogStoreEntry(type, length, actualChecksum, record.get())));

            } catch (IllegalStateException ise) {
                return Optional.empty();
            }
        }
    }

    private Optional<LogStoreRecord> readLogStoreRecord(byte[] recordBytes) {
        try {

            String key = null;
            String value = null;

            ByteBuffer recordBuffer = ByteBuffer.wrap(recordBytes);

            byte[] keyBytes = new byte[recordBuffer.getInt()];
            recordBuffer.get(keyBytes);

            key = new String(keyBytes, "utf-8");

            byte[] valueBytes = new byte[recordBuffer.getInt()];
            recordBuffer.get(valueBytes);

            value = new String(valueBytes, "utf-8");

            return Optional.of(new LogStoreRecord(key, value));

        } catch (UnsupportedEncodingException e) {
        }

        return Optional.empty();
    }

}
