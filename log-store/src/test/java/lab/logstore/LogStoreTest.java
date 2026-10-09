package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class LogStoreTest {
    @Test 
    public void rejectAppendWithBlankKey() {
        IllegalArgumentException exp = assertThrows(IllegalArgumentException.class, 
            () -> new InMemoryStore().write(null, null));

        assertEquals(InMemoryStore.ERR_ILLEGAL_KEY, exp.getMessage());
    }
}
