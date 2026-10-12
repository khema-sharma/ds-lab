package lab.logstore;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class AppendOnlyLogStoreReader implements Closeable {
    public static final String RECORD_SEP = "\n";

    private static final int EOF = -1;

    private final Checksum crc32Checksum;

    private final File logStore;
    private final RandomAccessFile storeAccessFile;

    private final LogStoreDataInput storeDataInput;

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

        storeDataInput = createRandomAccessStoreDataInput();
    }

    @Override
    public void close() throws IOException {
        storeAccessFile.close();
    }

    public Optional<LogStoreRecord> readRecordAt(long offset) throws IOException {
        storeAccessFile.seek(offset);

        ReadStoreEntryResult result = readStoreEntryAt(offset, storeDataInput);

        return (result.hasLogStoreEntry() ? Optional.of(result.getLogStoreEntry().record()) : Optional.empty());
    }

    private ReadStoreEntryResult readStoreEntryAt(long offset, LogStoreDataInput input) throws IOException {
        Optional<LogStoreEntry> emptyEntry = Optional.empty();

        StringBuffer lengthBuffer = new StringBuffer();

        long entryOffset = offset;
        int bytesRead = 0;

        bytesRead += readStoreEntryItem(lengthBuffer, input);

        if (lengthBuffer.isEmpty()) {
            return new ReadStoreEntryResult(bytesRead, emptyEntry);
        }

        StringBuffer checksumBuffer = new StringBuffer();
        bytesRead += readStoreEntryItem(checksumBuffer, input);

        if (checksumBuffer.isEmpty()) {
            return new ReadStoreEntryResult(bytesRead, emptyEntry);
        }

        int length = Integer.valueOf(lengthBuffer.toString());
        long checksum = Long.valueOf(checksumBuffer.toString());

        byte[] contents = input.read(length);
        bytesRead +=  contents.length;

        if (contents.length < length) {
            bytesRead += seekTillLineFeed(input);
            return new ReadStoreEntryResult(bytesRead, emptyEntry);
        }

        crc32Checksum.reset();
        crc32Checksum.update(contents);

        long actualChecksum = crc32Checksum.getValue();
        if (checksum != actualChecksum) {
            bytesRead += seekTillLineFeed(input);
            return new ReadStoreEntryResult(bytesRead, emptyEntry);
        }

        String keyValuePair = new String(contents, "utf-8");
        int valueIndex = keyValuePair.indexOf(":");

        String aKey = keyValuePair.substring(0, valueIndex);
        String keyValue = keyValuePair.substring(valueIndex + 1);

        LogStoreEntry entry = new LogStoreEntry(entryOffset, length, actualChecksum, new LogStoreRecord(aKey, keyValue));

        bytesRead += seekTillLineFeed(input);
        
        return new ReadStoreEntryResult(bytesRead, Optional.of(entry));
    }

    private long seekTillLineFeed(LogStoreDataInput input) throws IOException {
        long bytesRead = 0;

        int nByte = input.read();
        if (nByte != EOF) {
            bytesRead++;
        }

        while (nByte != EOF && !Character.toString(nByte).equals(RECORD_SEP)) {
            nByte = input.read();

            if (nByte != EOF) {
                bytesRead++;
            }
        }

        return bytesRead;
    }

    private int readStoreEntryItem(StringBuffer recordEntryBuffer, LogStoreDataInput input)
            throws IOException {
        int bytesRead = 0;

        int nByte = input.read();
        if (nByte != EOF) {
            bytesRead++;
        }

        StringBuffer tempBuffer = new StringBuffer();
        while (nByte != EOF
                && !Character.toString(nByte).equals(LogStoreEntry.RECORD_ENTRY_SEP)
                && !Character.toString(nByte).equals(RECORD_SEP)) {

            tempBuffer.append(Character.toString(nByte));

            nByte = input.read();

            if (nByte != EOF) {
                bytesRead++;
            }
        }

        if (nByte != EOF && Character.toString(nByte).equals(LogStoreEntry.RECORD_ENTRY_SEP)) {
            recordEntryBuffer.append(tempBuffer);
        }

        return bytesRead;
    }

    private LogStoreDataInput createRandomAccessStoreDataInput() {
        return new LogStoreDataInput() {

            @Override
            public int read() throws IOException {
                int bRead = storeAccessFile.read();
                return (bRead != -1 ? bRead : 0);
            }

            @Override
            public byte[] read(int length) throws IOException {
                byte[] contents = new byte[length];

                int bRead = storeAccessFile.read();
                int index = 0;

                do {
                    if (bRead != -1) {
                        contents[index++] = (byte) bRead;
                        bRead = storeAccessFile.read();
                    }
                } while (bRead != -1);

                byte[] actualContents = contents;
                if (index < length) {
                    actualContents = new byte[index];

                    for (int i = 0; i < index; i++) {
                        actualContents[i] = contents[i];
                    }
                }

                return actualContents;
            }

        };
    }

    private record ReadStoreEntryResult(long bytesRead, Optional<LogStoreEntry> entry) {
        public boolean hasLogStoreEntry() {
            return entry.isPresent();
        }

        public LogStoreEntry getLogStoreEntry() {
            return entry.get();
        }
    }

    private interface LogStoreDataInput {
        public int read() throws IOException;
        public byte[] read(int length) throws IOException;
    }

    public class AppendOnlyLogStoreIterator implements Iterator<LogStoreEntry> {

        private long pos = 0;

        private FileInputStream storeStream = null;
        private BufferedInputStream bufferedStoreStream = null;

        private final LogStoreDataInput bufferedStreamInput;

        public AppendOnlyLogStoreIterator() {
            try {
                storeStream = new FileInputStream(logStore);
                bufferedStoreStream = new BufferedInputStream(storeStream);

            } catch (FileNotFoundException e) {
                pos = -1;
            }

            bufferedStreamInput = createBufferedStreamInput();
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

        private LogStoreDataInput createBufferedStreamInput() {
            return new LogStoreDataInput() {

                @Override
                public int read() throws IOException {
                    int bRead = bufferedStoreStream.read();
                    return (bRead == -1 ? 0 : bRead);
                }

                @Override
                public byte[] read(int length) throws IOException {
                    return bufferedStoreStream.readNBytes(length);
                }

            };
        }

        private Optional<LogStoreEntry> readNext() {
            if (pos < 0) {
                return Optional.empty();
            }

            try {
                long lastPos = pos;
                 
                ReadStoreEntryResult result = readStoreEntryAt(lastPos, bufferedStreamInput);
                pos += result.bytesRead();

                if (pos == lastPos) {
                    pos = EOF;
                }

                return result.entry();
            } catch (IOException e) {
                pos = EOF;
            } finally {
                if (pos == EOF) {
                    close();
                }
            }

            return null;
        }

    }

}
