package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class LogStoreEntryInput extends RandomLogStoreAccess {

    private final Object readMonitor;

    public LogStoreEntryInput(File file) throws FileNotFoundException {
        super(file, "r");

        readMonitor = new Object();
    }

    public Optional<LogStoreEntrySet<LogStoreRecord>> getEntryAt(long offset) throws IOException {
        if (offset < 0 || offset > length()) {
            throw new IllegalArgumentException("offset must be between 0 and length of the store.");
        }

        synchronized (readMonitor) {
            seek(offset);
            return getRecordEntry();
        }
    }

    public Optional<LogStoreEntrySet<LogStoreCDCRecord>> getCDCEntryAt(long offset) throws IOException {
        if (offset < 0 || offset > length()) {
            throw new IllegalArgumentException("offset must be between 0 and length of the store.");
        }

        synchronized (readMonitor) {
            seek(offset);
            return getCDCEntry();
        }
    }

    public List<LogStoreEntrySet<LogStoreRecord>> getCompleteLogEntries() throws IOException {
        long lastPos = pos();
        List<LogStoreEntrySet<LogStoreRecord>> allRecordEntries = new LinkedList<>();

        while (!isEOF()) {
            long previousPos = pos();

            try {
                Optional<LogStoreEntrySet<LogStoreRecord>> recordEntrySet = getRecordEntry();

                if (recordEntrySet.isPresent()) {
                    if (!recordEntrySet.get().entry().isLogEntry()) {
                        break;
                    }

                    Optional<LogStoreEntrySet<LogStoreCDCRecord>> cdcEntrySet = getCDCEntry();

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

    Optional<LogStoreEntrySet<LogStoreRecord>> getRecordEntry() throws IOException {
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

                Optional<LogStoreRecord> record = LogStoreRecord.createFromPayload(recordBytes);

                if (record.isEmpty()) {
                    throw new IllegalStateException("Could not parse record from the stream.");
                }

                long entryLength = (pos() - entryOffset);

                LogStoreEntry<LogStoreRecord> recordEntry = new LogStoreEntry<LogStoreRecord>(type, length,
                        actualChecksum, record.get());

                LogStoreEntrySet<LogStoreRecord> entrySet = new LogStoreEntrySet<LogStoreRecord>(entryOffset,
                        entryLength, recordEntry);

                return Optional.of(entrySet);

            } catch (IllegalStateException ise) {
                System.err.println("An error occurred while reading record entry: " + ise.getMessage());
            }

            return Optional.empty();
        }
    }

    Optional<LogStoreEntrySet<LogStoreCDCRecord>> getCDCEntry() throws IOException {
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

                Optional<LogStoreCDCRecord> record = LogStoreCDCRecord.createFromPayload(recordBytes);

                if (record.isEmpty()) {
                    throw new IllegalStateException("Could not parse record from the stream.");
                }

                long entryLength = (pos() - entryOffset);

                LogStoreEntry<LogStoreCDCRecord> recordEntry = new LogStoreEntry<LogStoreCDCRecord>(type, length,
                        actualChecksum, record.get());

                LogStoreEntrySet<LogStoreCDCRecord> entrySet = new LogStoreEntrySet<LogStoreCDCRecord>(entryOffset,
                        entryLength, recordEntry);

                return Optional.of(entrySet);

            } catch (IllegalStateException ise) {
                System.err.println("An error occurred while reading record entry: " + ise.getMessage());
            }

            return Optional.empty();
        }
    }

}
