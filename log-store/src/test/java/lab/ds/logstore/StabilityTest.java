package lab.ds.logstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import lab.ds.logstore.core.IdempotencyKey;

@ExtendWith(MockitoExtension.class)
public class StabilityTest {

    /**
     * This test asserts failure modes handled by {@code AppendOnlyLogStoreClient}.
     * <p>
     * It asserts that the client:
     * <ul>
     * <li>throws {@code IOException} when it fails to save the record before
     * opening the circuit
     * <li>throws {@code StoreBusyException} when the circuit is open
     * <li>throws {@code IOException} again when it fails to save when circuit was
     * half open
     * <li>the {@code get} call returns the value if save was successful when
     * circuit was half open
     * <li>never calls the non-idempotent write when it reties several times before
     * opening the circuit
     * </ul>
     * 
     * @throws StoreInitializationException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    @DisplayName("Store client with circuit breaker")
    public void assertsStoreClientFailureHandling()
            throws StoreInitializationException, IOException, InterruptedException {

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        Answer<Long> mockedPutAnswer = new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return 1L;
            }
        };

        for (int i = 0; i < 3; i++) {
            AppendOnlyLogStore mockedStore = mock(AppendOnlyLogStore.class);

            doThrow(IOException.class)
                    .doThrow(IOException.class)
                    // Trips the circuit to open state
                    .doThrow(IOException.class)
                    // Called after 1st wait, trips back to open state from half-open state
                    .doThrow(IOException.class)
                    // Called after 2nd wait, closes the circuit
                    .doAnswer(mockedPutAnswer)
                    .when(mockedStore)
                    .put(any(IdempotencyKey.class), anyString(), anyString());

            when(mockedStore.get(anyString())).thenReturn("value-1");

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            // Three failures in sequence must open the circuit
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));

            // Exception thrown by client when circuit is open
            // Call should never reach the mocked store
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalWaitUntilStateIsSwitched();

            // Circuit must be half-open, mocked store will call the actual store's method
            // and trip back
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));

            // Unlike closed state, half-open state will open the circuit immediately
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalWaitUntilStateIsSwitched();

            // This closes the circuit
            storeClient.put("key-1", "value-1");

            storeClient.internalWaitUntilStateIsSwitched();
            
            assertEquals(true, storeClient.allNodesClosed());
            assertEquals("value-1", storeClient.get("key-1"));

            // Assert that non-idempotent write never happens
            for (AppendOnlyLogStore mockedStore : stores) {
                verify(mockedStore, never()).put(anyString(), anyString());
            }

        }

    }

}
