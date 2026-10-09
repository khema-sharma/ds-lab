package lab.logstore;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;

public class FileLogStore implements LogStore, Closeable {

    private File store;
    private FileWriter storeWriter;

    public FileLogStore() throws IOException {
        initStore();
    }

    @Override
    public void write(String key, String value) throws IOException {
        storeWriter.write(MessageFormat.format("{0}:{1}\n", key, value));
        storeWriter.flush();
    }

    @Override
    public String read(String key) throws IOException {
        try (FileReader storeReader = new FileReader(store)) {
            BufferedReader bufferedReader = new BufferedReader(storeReader);
            String line = bufferedReader.readLine();

            while (line != null) {
                String[] keyAndValue = line.split(":");

                if (key.equals(keyAndValue[0])) {
                    return keyAndValue[1];
                }

                line = bufferedReader.readLine();
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
        store = File.createTempFile("write-ahead", ".db");
        storeWriter = new FileWriter(store);

        System.out.println("Created store at: " + store);
    }

    public static void main(String[] args) {
        try (FileLogStore store = new FileLogStore()) {
            System.out.println(store.benchmark());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
