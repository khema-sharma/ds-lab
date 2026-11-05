package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StabilityTest {

    @Test
    @DisplayName("Store client with circuit breaker")
    public void rejectsStoreClientWithCircuitBreaker()
            throws StoreInitializationException, IOException, InterruptedException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        for (int i = 0; i < 3; i++) {
            AppendOnlyLogStore mockedStore = mock(AppendOnlyLogStore.class);

            doThrow(IOException.class)
                    .doThrow(IOException.class)
                    // Trips the circuit
                    .doThrow(IOException.class)
                    // Called after sleep, trips back from half-open state
                    .doThrow(IOException.class)
                    // Called after sleep, resets from half-open state
                    .doCallRealMethod()
                    .when(mockedStore)
                    .put(anyString(), anyString());

            when(mockedStore.get(anyString())).thenReturn("value-1");

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            // Three failures in sequence must open the circuit
            for (int i = 0; i < 3; i++) {
                assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));
            }

            // Exception thrown by client when circuit is open
            // Call should never reach the mocked store
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalBlockUntilCircuitIsOpen();

            // Circuit must be half-open, mocked store will call the actual store's method and trip back
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));

            // Unlike closed state, half-open state will open the circuit immediately
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalBlockUntilCircuitIsOpen();

            // This closes the circuit
            assertEquals("value-1", storeClient.get("key-1"));
        }
    }

}
