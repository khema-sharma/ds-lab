package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

public class InMemoryStoreTest {
    @Test
    public void rejectAppendWithBlankKey() {
        IllegalArgumentException exp = assertThrows(IllegalArgumentException.class,
                () -> new InMemoryStore().put(null, null));

        assertEquals(InMemoryStore.ERR_ILLEGAL_KEY, exp.getMessage());
    }

    @Test
    public void acceptsCompletedWrite() throws IOException {
        InMemoryStore store = new InMemoryStore();
        store.put("key-1", "value-1");

        assertEquals("value-1", store.get("key-1"));
    }

    @Test
    public void acceptsReadVisibility() throws InterruptedException, IOException, ExecutionException {
        InMemoryStore store = new InMemoryStore();
        store.put("key-1", "value-1");

        final ExecutorService service = Executors.newFixedThreadPool(2);

        service.execute(() -> {
            try {
                Thread.sleep(2);
                store.put("key-1", "value-2");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        final Set<String> observedValues = new HashSet<>();

        service.submit(() -> {
            try {
                String value = null;

                do {
                    value = store.get("key-1");
                    observedValues.add(value);

                } while (value != null && !value.equals("value-2"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).get();

        // Guarantee that read never observes a broken value i.e. store must return only
        // "value-1" until "value-2" write is completed
        for (String observedValue : observedValues) {
            assertTrue(observedValue.equals("value-1") || observedValue.equals("value-2"));
        }

        // Ensure that the reader thread sees value-2 written by writer thread
        assertTrue(observedValues.contains("value-2"));
    }

}
