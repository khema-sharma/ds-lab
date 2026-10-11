package lab.logstore;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.StringTokenizer;
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
        try (FileReader storeReader = new FileReader(store)) {
            BufferedReader bufferedReader = new BufferedReader(storeReader);
            String line = bufferedReader.readLine();

            while (line != null) {
                try {
                    StringTokenizer tokenizer = new StringTokenizer(line, ",");
                    
                    String lengthString = tokenizer.nextToken();
                    String checksumString = tokenizer.nextToken();

                    int length = Integer.valueOf(lengthString);
                    long checksum = Long.valueOf(checksumString);

                    int contentsIndex = line.indexOf(checksumString) + checksumString.length() + 1;
                    String contents = line.substring(contentsIndex);

                    crc32Checksum.reset();
                    crc32Checksum.update(contents.getBytes("utf-8"));
                    
                    if (length != contents.getBytes().length) {
                        continue;
                    }
                    
                    long actualChecksum = crc32Checksum.getValue();
                    if (checksum != actualChecksum) {
                        continue;
                    }

                    int valueIndex = contents.indexOf(":");
                    String[] keyValuePair = new String[] { 
                        contents.substring(0, valueIndex),
                        contents.substring(valueIndex + 1)
                    };
                    
                    if (key.equals(keyValuePair[0])) {
                        return keyValuePair[1];
                    }
                } catch (Exception e) {
                    continue;
                } finally {
                    line = bufferedReader.readLine();
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
