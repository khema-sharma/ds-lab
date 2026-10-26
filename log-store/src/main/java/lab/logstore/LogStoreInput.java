package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class LogStoreInput extends RandomLogStoreAccess {

    private final Object readMonitor;

    public LogStoreInput(File file) throws FileNotFoundException {
        super(file, "r");

        readMonitor = new Object();
    }

    public Optional<LogStoreEntrySet> getEntryAt(long offset) throws IOException {
        if (offset < 0 || offset > length()) {
            throw new IllegalArgumentException("offset must be between 0 and length of the store.");
        }

        synchronized (readMonitor) {
            seek(offset);
            return getDataEntry();
        }
    }

    public Optional<LogStoreEntrySet> getCdcEntryAt(long offset) throws IOException {
        if (offset < 0 || offset > length()) {
            throw new IllegalArgumentException("offset must be between 0 and length of the store.");
        }

        synchronized (readMonitor) {
            seek(offset);
            return getCdcEntry();
        }
    }

    public List<LogStoreEntrySet> getCompleteLogEntries() throws IOException {
        long lastPos = pos();
        List<LogStoreEntrySet> allRecordEntries = new LinkedList<>();

        while (!isEOF()) {
            long previousPos = pos();

            try {
                Optional<LogStoreEntrySet> recordEntrySet = getDataEntry();

                if (recordEntrySet.isPresent()) {
                    if (!(recordEntrySet.get().entry() instanceof DataEntry)) {
                        break;
                    }

                    Optional<LogStoreEntrySet> cdcEntrySet = getCdcEntry();

                    if (cdcEntrySet.isPresent()) {
                        if (!(cdcEntrySet.get().entry() instanceof CdcEntry)) {
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

    Optional<LogStoreEntrySet> getDataEntry() throws IOException {
        return getLogEntry(LogStoreEntry.TYPE_DATA_ENTRY);
    }

    Optional<LogStoreEntrySet> getCdcEntry() throws IOException {
        return getLogEntry(LogStoreEntry.TYPE_CDC_ENTRY);
    }

    private Optional<LogStoreEntrySet> getLogEntry(byte expectedType) throws IOException {
        long entryOffset = pos();

        synchronized (readMonitor) {
            try {
                byte type = (byte) readByte();

                if (type != expectedType) {
                    throw new IllegalStateException(
                            "Read unexpected type from the stream, expected " + expectedType + ", read " + type);
                }

                int length = readInt();
                long expectedChecksum = readLong();
                byte[] recordBytes = new byte[length];

                readFully(recordBytes);

                long actualChecksum = getChecksum(recordBytes);
                if (expectedChecksum != actualChecksum) {
                    throw new IllegalStateException("Invalid record, checksum mismatch.");
                }

                return switch (type) {
                    case LogStoreEntry.TYPE_DATA_ENTRY ->
                        getDataEntry(entryOffset, length, actualChecksum, recordBytes);
                    case LogStoreEntry.TYPE_CDC_ENTRY -> getCdcEntry(entryOffset, length, actualChecksum, recordBytes);
                    default -> throw new IllegalStateException();
                };

            } catch (IllegalStateException ise) {
                System.err.println("An error occurred while reading record entry: " + ise.getMessage());
            }

            return Optional.empty();
        }
    }

    private Optional<LogStoreEntrySet> getDataEntry(long entryOffset, int length, long actualChecksum,
            byte[] recordBytes) throws IOException {
        Optional<DataRecord> record = DataRecord.createFromPayload(recordBytes);

        if (record.isEmpty()) {
            throw new IllegalStateException("Could not parse record from the stream.");
        }

        long entryLength = (pos() - entryOffset);

        return Optional.of(new LogStoreEntrySet(entryOffset, entryLength,
                new DataEntry(length, actualChecksum, record.get())));
    }

    private Optional<LogStoreEntrySet> getCdcEntry(long entryOffset, int length, long actualChecksum,
            byte[] recordBytes) throws IOException {
        Optional<CdcRecord> record = CdcRecord.createFromPayload(recordBytes);

        if (record.isEmpty()) {
            throw new IllegalStateException("Could not parse record from the stream.");
        }

        long entryLength = (pos() - entryOffset);

        return Optional.of(new LogStoreEntrySet(entryOffset, entryLength,
                new CdcEntry(length, actualChecksum, record.get())));
    }

}
