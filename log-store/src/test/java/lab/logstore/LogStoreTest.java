package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class LogStoreTest {
    @Test 
    public void rejectAppendWithBlankKey() {
        IllegalArgumentException exp = assertThrows(IllegalArgumentException.class, 
            () -> new InMemoryLogStore().write(null, null));

        assertEquals(InMemoryLogStore.ERR_ILLEGAL_KEY, exp.getMessage());
    }
}
