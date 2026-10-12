package lab.logstore;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class AppendOnlyLogStoreReader implements Closeable {
    public static final String RECORD_SEP = "\n";

    private static final int EOF = -1;

    private final Checksum crc32Checksum;

    private final File logStore;
    private final RandomAccessFile storeAccessFile;

    public AppendOnlyLogStoreReader(File store) throws StoreInitializationException {
        if (store == null) {
            throw new IllegalArgumentException("store cannot be null.");
        }

        logStore = store;

        try {
            storeAccessFile = new RandomAccessFile(store, "r");

        } catch (FileNotFoundException fne) {
            throw new StoreInitializationException(fne);
        }

        crc32Checksum = new CRC32();
    }

    @Override
    public void close() throws IOException {
        storeAccessFile.close();
    }

    public Optional<LogStoreRecord> readRecordAt(long offset) throws IOException {
        Optional<LogStoreEntry> optionalEntry = readStoreEntryAt(offset);
        return (optionalEntry.isPresent() ? Optional.of(optionalEntry.get().record()) : Optional.empty());
    }

    private Optional<LogStoreEntry> readStoreEntryAt(long offset) throws IOException {
        StringBuffer lengthBuffer = new StringBuffer();

        storeAccessFile.seek(offset);

        readStoreEntryItem(lengthBuffer);

        if (lengthBuffer.isEmpty()) {
            return Optional.empty();
        }

        StringBuffer checksumBuffer = new StringBuffer();
        readStoreEntryItem(checksumBuffer);

        if (checksumBuffer.isEmpty()) {
            return Optional.empty();
        }

        int length = Integer.valueOf(lengthBuffer.toString());
        long checksum = Long.valueOf(checksumBuffer.toString());

        byte[] contents = new byte[length];
        int actualLength = storeAccessFile.read(contents);
        
        if (actualLength < length) {
            return Optional.empty();
        }

        crc32Checksum.reset();
        crc32Checksum.update(contents);

        long actualChecksum = crc32Checksum.getValue();
        if (checksum != actualChecksum) {
            return Optional.empty();
        }

        String keyValuePair = new String(contents, "utf-8");
        int valueIndex = keyValuePair.indexOf(":");

        String aKey = keyValuePair.substring(0, valueIndex);
        String keyValue = keyValuePair.substring(valueIndex + 1);

        return Optional.of(new LogStoreEntry(offset, length, actualChecksum, new LogStoreRecord(aKey, keyValue)));
    }

    private int readStoreEntryItem(StringBuffer recordEntryBuffer) throws IOException {
        int nByte = storeAccessFile.read();

        StringBuffer tempBuffer = new StringBuffer();
        while (nByte != EOF
                && !Character.toString(nByte).equals(LogStoreEntry.RECORD_ENTRY_SEP)
                && !Character.toString(nByte).equals(RECORD_SEP)) {

            tempBuffer.append(Character.toString(nByte));

            nByte = storeAccessFile.read();
        }

        if (nByte != EOF && Character.toString(nByte).equals(LogStoreEntry.RECORD_ENTRY_SEP)) {
            recordEntryBuffer.append(tempBuffer);
        }

        return nByte;
    }

    public class LogStoreIterator implements Iterator<LogStoreEntry> {

        private int pos = 0;

        private FileInputStream storeStream = null;
        private BufferedInputStream bufferedStoreStream = null;

        public LogStoreIterator() {
            try {
                storeStream = new FileInputStream(logStore);
                bufferedStoreStream = new BufferedInputStream(storeStream);
            } catch (FileNotFoundException e) {
                pos = -1;
            }
        }

        @Override
        public boolean hasNext() {
            return (pos != EOF);
        }

        @Override
        public LogStoreEntry next() {
            Optional<LogStoreEntry> optionalEntry = readNext();
            return (optionalEntry.isEmpty() ? null : optionalEntry.get());
        }

        private void close() {
            try {
                storeStream.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        private Optional<LogStoreEntry> readNext() {
            if (pos < 0) {
                return Optional.empty();
            }

            LogStoreEntry[] entry = new LogStoreEntry[] { null };

            try {
                int lastPos = pos;
                pos = readStoreEntry(entry);

                if (pos == lastPos) {
                    pos = EOF;
                }

                return Optional.ofNullable(entry[0]);
            } catch (IOException e) {
                pos = EOF;
            } finally {
                if (pos == EOF) {
                    close();
                }
            }

            return null;
        }

        private int readStoreEntry(LogStoreEntry[] entry) throws IOException {
            StringBuffer lengthBuffer = new StringBuffer();

            int entryOffset = pos;
            int offset = pos;
            
            offset += readStoreEntryItem(lengthBuffer);

            if (lengthBuffer.isEmpty()) {
                return offset;
            }

            StringBuffer checksumBuffer = new StringBuffer();
            offset += readStoreEntryItem(checksumBuffer);

            if (checksumBuffer.isEmpty()) {
                return offset;
            }

            int length = Integer.valueOf(lengthBuffer.toString());
            long checksum = Long.valueOf(checksumBuffer.toString());

            byte[] contents = bufferedStoreStream.readNBytes(length);
            offset += contents.length;

            if (contents.length < length) {
                offset += seekTillLineFeed(bufferedStoreStream);
                return offset;
            }

            crc32Checksum.reset();
            crc32Checksum.update(contents);

            long actualChecksum = crc32Checksum.getValue();
            if (checksum != actualChecksum) {
                offset += seekTillLineFeed(bufferedStoreStream);
                return offset;
            }

            String keyValuePair = new String(contents, "utf-8");
            int valueIndex = keyValuePair.indexOf(":");

            String aKey = keyValuePair.substring(0, valueIndex);
            String keyValue = keyValuePair.substring(valueIndex + 1);

            entry[0] = new LogStoreEntry(entryOffset, length, actualChecksum, new LogStoreRecord(aKey, keyValue));

            offset += seekTillLineFeed(bufferedStoreStream);
            return offset;
        }

        private int seekTillLineFeed(BufferedInputStream bis) throws IOException {
            int skips = 0;
            
            int nByte = bis.read();
            if (nByte != EOF) {
                skips++;
            }

            while (nByte != EOF && !Character.toString(nByte).equals(RECORD_SEP)) {
                nByte = bis.read();

                if (nByte != EOF) {
                    skips++;
                }
            }

            return skips;
        }

        private int readStoreEntryItem(StringBuffer recordEntryBuffer) throws IOException {
            int bytesRead = 0;
            
            int nByte = bufferedStoreStream.read();
            if (nByte != EOF) {
                bytesRead++;
            }

            StringBuffer tempBuffer = new StringBuffer();
            while (nByte != EOF
                    && !Character.toString(nByte).equals(LogStoreEntry.RECORD_ENTRY_SEP)
                    && !Character.toString(nByte).equals(RECORD_SEP)) {

                tempBuffer.append(Character.toString(nByte));

                nByte = bufferedStoreStream.read();

                if (nByte != EOF) {
                    bytesRead++;
                }
            }

            if (nByte != EOF && Character.toString(nByte).equals(LogStoreEntry.RECORD_ENTRY_SEP)) {
                recordEntryBuffer.append(tempBuffer);
            }

            return bytesRead;
        }

    }

}
