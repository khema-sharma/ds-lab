package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.jupiter.api.Test;

public class LogStoreTest {
    @Test 
    public void rejectAppendWithBlankKey() {
        IllegalArgumentException exp = assertThrows(IllegalArgumentException.class, 
            () -> new InMemoryStore().write(null, null));

        assertEquals(InMemoryStore.ERR_ILLEGAL_KEY, exp.getMessage());
    }

    @Test
    public void rejectsTornWriteOfSameLength() throws StoreInitializationException, IOException {
        String storePath = createStoreAndAddRecords();

        tearRecord(storePath, "@#$^");

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.read("key-1"));
            assertEquals(null, store.read("key-2"));
            assertEquals("value-3", store.read("key-3"));
        }
    }

    @Test
    public void rejectsTornWrite() throws StoreInitializationException, IOException {
        String storePath = createStoreAndAddRecords();

        tearRecord(storePath, "@#$^$%^&&*&&##@@@$$%%%\0\n");

        try (AppendOnlyLogStore store = new AppendOnlyLogStore(storePath)) {
            assertEquals("value-1", store.read("key-1"));
            assertEquals(null, store.read("key-2"));
            assertEquals(null, store.read("key-3"));
        }
    }

    private void tearRecord(String storePath, String junkChars) throws IOException {
        try (RandomAccessFile storeFile = new RandomAccessFile(storePath, "rw")) {
            storeFile.seek(50);
            storeFile.write(junkChars.getBytes());
        }
    }

    private String createStoreAndAddRecords() throws StoreInitializationException, IOException {
        String storePath = null;

        try (AppendOnlyLogStore store = new AppendOnlyLogStore()) {
            storePath = store.getPath();

            store.write("key-1", "value-1");
            store.write("key-2", "value-2");
            store.write("key-3", "value-3");

        }

        return storePath;
    }
}
