package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

public class InMemoryStoreTest {
    @Test
    public void rejectAppendWithBlankKey() {
        IllegalArgumentException exp = assertThrows(IllegalArgumentException.class,
                () -> new InMemoryStore().write(null, null));

        assertEquals(InMemoryStore.ERR_ILLEGAL_KEY, exp.getMessage());
    }

    @Test
    public void acceptsCompletedWrite() throws IOException {
        InMemoryStore store = new InMemoryStore();
        store.write("key-1", "value-1");
        
        assertEquals("value-1", store.read("key-1"));
    }

    @Test
    public void acceptsReadVisibility() throws InterruptedException, IOException, ExecutionException {
        InMemoryStore store = new InMemoryStore();
        store.write("key-1", "value-1");

        final ExecutorService service = Executors.newFixedThreadPool(2);
        
        service.execute(() -> {
            try {
                Thread.sleep(2);
                store.write("key-1", "value-2");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        service.submit(() -> {
            try {
                String value = null;
                
                do {
                    value = store.read("key-1");

                } while (value != null && !value.equals("value-2"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).get();

        assertEquals("value-2", store.read("key-1"));
    }

}
