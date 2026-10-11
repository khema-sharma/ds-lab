package lab.logstore;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class AppendOnlyLogStore extends AbstractStore implements Closeable {

    private static final String ERR_IN_INIT = "An error occurred while initializing store: {0}";

    private static final String KEY_VALUE_SEP = ":";
    private static final String KEY_VALUE_PATTERN = "{0}" + KEY_VALUE_SEP + "{1}";
    private static final String RECORD_ENTRY_SEP = ",";
    private static final String RECORD_ENTRY_PATTERN = "{0}" + RECORD_ENTRY_SEP + "{1}" + RECORD_ENTRY_SEP + "{2}";

    private String storePath;

    private File store;
    private FileWriter storeWriter;

    private final Checksum crc32Checksum = new CRC32();

    public AppendOnlyLogStore(String path) throws StoreInitializationException {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Store path cannot be null.");
        }

        storePath = path;
        
        try {
            initStore();

        } catch (Exception e) {
            throw new StoreInitializationException(
                    MessageFormat.format(ERR_IN_INIT, e.getMessage()), e);
        }
    }

    public AppendOnlyLogStore() throws StoreInitializationException {
        try {
            initStore();

        } catch (Exception e) {
            throw new StoreInitializationException(
                    MessageFormat.format(ERR_IN_INIT, e.getMessage()), e);
        }
    }

    public String getPath() {
        return store.getAbsolutePath();
    }

    @Override
    public void write(String key, String value) throws IOException {
        String record = MessageFormat.format(KEY_VALUE_PATTERN, key, value);

        crc32Checksum.reset();
        crc32Checksum.update(record.getBytes("utf-8"));

        String length = Integer.toString(record.getBytes("utf-8").length);
        String checksum = Long.toString(crc32Checksum.getValue());

        String recordWithMetadata = MessageFormat.format(RECORD_ENTRY_PATTERN + "\n", length, checksum, record);

        storeWriter.write(recordWithMetadata);
        storeWriter.flush();
    }

    @Override
    public String read(String key) throws IOException {
        try (FileInputStream storeStream = new FileInputStream(store)) {
            BufferedInputStream bis = new BufferedInputStream(storeStream);
            
            String[] keyAndValue = new String[] { null, null };
            while (readRecord(bis, keyAndValue) != -1) {
                if (keyAndValue[0].equals(key)) {
                    return keyAndValue[1];
                }
            }
        }

        return null;
    }

    @Override
    public void clear() throws IOException {
        initStore();
    }

    public void close() throws IOException {
        storeWriter.close();
    }

    private void initStore() throws IOException {
        store = (storePath == null ? 
                    File.createTempFile("write-ahead", ".db") :
                    new File(storePath));
        storeWriter = new FileWriter(store, true);

        System.out.println("Created store at: " + store);
    }

    private int readRecord(BufferedInputStream bis, String[] keyAndValue) throws IOException {
        int length = Integer.valueOf(readRecordEntry(bis));
        long checksum = Long.valueOf(readRecordEntry(bis));
    
        byte[] contents = bis.readNBytes(length);
        if (contents.length < length) {
            return seekTillEnd(bis);
        }

        crc32Checksum.reset();
        crc32Checksum.update(contents);

        long actualChecksum = crc32Checksum.getValue();
        if (checksum != actualChecksum) {
            return seekTillEnd(bis);
        }

        String keyValuePair = new String(contents, "utf-8");
        int valueIndex = keyValuePair.indexOf(":");
        
        keyAndValue[0] = keyValuePair.substring(0, valueIndex);
        keyAndValue[1] = keyValuePair.substring(valueIndex + 1);

        return seekTillEnd(bis);
    }

    private int seekTillEnd(BufferedInputStream bis) throws IOException {
        int rByte = bis.read();
        while (rByte != -1 && !Character.toString(rByte).equals("\n")) {
            rByte = bis.read();
        }

        return rByte;
    }

    private String readRecordEntry(BufferedInputStream bis) throws IOException {
        String entry = "";
        
        int rByte = bis.read();
        while (rByte != -1 && !Character.toString(rByte).equals(RECORD_ENTRY_SEP)) {
            entry += Character.toString(rByte);
            rByte = bis.read();
        }

        if (rByte == -1) {
            return null;
        }

        return entry;
    }

    public static void main(String[] args) {
        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            OperationBenchmark[] benchMarks = store.benchmarkOperations();

            System.out.println("Write benchmarks: " + benchMarks[0]);
            System.out.println("Read benchmarks: " + benchMarks[1]);
        } catch (StoreInitializationException | IOException e) {
            e.printStackTrace();
        }
    }

}
