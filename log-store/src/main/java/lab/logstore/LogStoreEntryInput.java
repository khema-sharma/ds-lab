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

    public Optional<LogStoreEntrySet> getEntryAt(long offset) throws IOException {
        if (offset < 0 || offset > length()) {
            throw new IllegalArgumentException("offset must be between 0 and length of the store.");
        }

        synchronized (readMonitor) {
            seek(offset);
            return getDataEntry();
        }
    }

    public Optional<LogStoreEntrySet> getCDCEntryAt(long offset) throws IOException {
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

                Optional<DataRecord> record = DataRecord.createFromPayload(recordBytes);

                if (record.isEmpty()) {
                    throw new IllegalStateException("Could not parse record from the stream.");
                }

                long entryLength = (pos() - entryOffset);

                return Optional.of(new LogStoreEntrySet(entryOffset, entryLength,
                        new DataEntry(length, actualChecksum, record.get())));

            } catch (IllegalStateException ise) {
                System.err.println("An error occurred while reading record entry: " + ise.getMessage());
            }

            return Optional.empty();
        }
    }

    Optional<LogStoreEntrySet> getCdcEntry() throws IOException {
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

                Optional<CdcRecord> record = CdcRecord.createFromPayload(recordBytes);

                if (record.isEmpty()) {
                    throw new IllegalStateException("Could not parse record from the stream.");
                }

                long entryLength = (pos() - entryOffset);

                return Optional.of(new LogStoreEntrySet(entryOffset, entryLength,
                        new CdcEntry(length, actualChecksum, record.get())));

            } catch (IllegalStateException ise) {
                System.err.println("An error occurred while reading record entry: " + ise.getMessage());
            }

            return Optional.empty();
        }
    }

}
