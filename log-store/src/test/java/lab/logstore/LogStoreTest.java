package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

public class LogStoreTest {
    @Test
    public void rejectAppendWithBlankKey() {
        IllegalArgumentException exp = assertThrows(IllegalArgumentException.class,
                () -> new InMemoryStore().write(null, null));

        assertEquals(InMemoryStore.ERR_ILLEGAL_KEY, exp.getMessage());
    }

    @Test
    public void acceptsValidWrites() throws StoreInitializationException, IOException {
        String storePath = createStoreAndAddRecords(new String[][] { { "key-1", "value-1" }, { "key-2", "value-2" } });

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.read("key-1"));
            assertEquals("value-2", store.read("key-2"));
        }
    }

    @Test
    public void rejectsTornWrite() throws StoreInitializationException, IOException {
        String storePath = createStoreAndAddRecords(new String[][] { { "key-1", "value-1" }, { "key-2", "value-2" } });

        tearRecord(storePath, 35);

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.read("key-1"));
            assertEquals(null, store.read("key-2"));
        }
    }

    @Test
    public void acceptsLatestWrite() throws StoreInitializationException, IOException {
        String storePath = createStoreAndAddRecords(new String[][] { { "key-1", "value-1" }, { "key-1", "value-2" } });

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-2", store.read("key-1"));
        }
    }

    @Test
    public void acceptsLastCompleteWrite() throws StoreInitializationException, IOException {
        String storePath = createStoreAndAddRecords(new String[][] { { "key-1", "value-1" }, { "key-1", "value-2" } });

        tearRecord(storePath, 50);

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.read("key-1"));
        }
    }

    private void tearRecord(String storePath, int retainTill) throws IOException {
        Path storePathObject = Path.of(URI.create("file:///" + storePath));

        String contents = Files.readString(storePathObject, StandardCharsets.UTF_8);
        String tornContents = contents.substring(0, retainTill);

        Files.writeString(storePathObject, tornContents, StandardCharsets.UTF_8);
    }

    private String createStoreAndAddRecords(String[][] records) throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            for (String[] record : records) {
                store.write(record[0], record[1]);
            }
        }

        return storePath;
    }
}
