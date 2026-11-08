package lab.ds.logstore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Stubber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lab.ds.logstore.core.IdempotencyKey;

@ExtendWith(MockitoExtension.class)
public class StabilityTest {

    private static Logger logger = LoggerFactory.getLogger(StabilityTest.class.getSimpleName());

    @Test
    public void assertsMajoritySuccess() throws IOException, StoreInitializationException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        AppendOnlyLogStore firstMockedStore = mockStore();

        doThrow(IOException.class)
                .when(firstMockedStore)
                .put(any(), anyString(), anyString());

        stores[0] = firstMockedStore;

        for (int i = 1; i <= 2; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> 1L)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void assertsMajorityFailure() throws IOException, StoreInitializationException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        AppendOnlyLogStore firstMockedStore = mockStore();

        doAnswer(invocation -> 1L)
                .when(firstMockedStore)
                .put(any(), anyString(), anyString());

        stores[0] = firstMockedStore;

        for (int i = 1; i <= 2; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void rejectsSlowMajority() throws IOException, StoreInitializationException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        for (int i = 0; i < 3; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> {
                Thread.sleep(Duration.ofSeconds(10));
                return 1L;
            }).when(mockedStore).put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(OperationStateUnknownException.class, () -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void assertsClosedToOpenTransition() throws IOException, StoreInitializationException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        for (int i = 0; i < 3; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void assertsHalfOpenToClosedTransition()
            throws IOException, StoreInitializationException, InterruptedException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        for (int i = 0; i < 3; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .doThrow(IOException.class)
                    .doThrow(IOException.class)
                    .doAnswer(invocation -> 1L)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalWaitUntilStateSwitchThreshold();

            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
        }
    }

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
    public void assertsStoreClientResliency()
            throws StoreInitializationException, IOException, InterruptedException {

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[3];

        for (int i = 0; i < 3; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            Stubber putStubber = null;

            if (i == 1) {
                putStubber = doThrow(IOException.class).doAnswer(invocation -> {
                    logger.info("Delaying IOException throw...");
                    Thread.sleep(Duration.ofSeconds(3));

                    throw new IOException();
                });
            } else {
                putStubber = doThrow(IOException.class).doThrow(IOException.class);
            }

            // Trips the circuit to open state
            putStubber.doThrow(IOException.class)
                    // Called after 1st wait, trips back to open state from half-open state
                    .doThrow(IOException.class);

            // Called after 2nd wait, closes the circuit
            if (i == 2) {
                putStubber.doAnswer(invocation -> {
                    logger.info("Delaying answer...");
                    Thread.sleep(Duration.ofSeconds(3));

                    return 1L;
                });
            } else {
                putStubber.doAnswer(invocation -> 1L);
            }

            putStubber.when(mockedStore).put(any(IdempotencyKey.class), anyString(), anyString());

            when(mockedStore.get(anyString())).thenReturn("value-1");

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            // Three failures in sequence must open the circuit
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));

            // Exception thrown by client when circuit is open
            // Call should never reach the mocked store
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalWaitUntilStateSwitchThreshold();

            // Circuit must be half-open, mocked store will call the actual store's method
            // and trip back
            assertThrows(IOException.class, () -> storeClient.put("key-1", "value-1"));

            // Unlike closed state, half-open state will open the circuit immediately
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalWaitUntilStateSwitchThreshold();

            // This closes the circuit
            storeClient.put("key-1", "value-1");

            assertEquals("value-1", storeClient.get("key-1"));

            // Assert that non-idempotent write never happens
            for (AppendOnlyLogStore mockedStore : stores) {
                verify(mockedStore, never()).put(anyString(), anyString());
            }

        }

    }

    private AppendOnlyLogStore mockStore() {
        AppendOnlyLogStore store = mock(AppendOnlyLogStore.class);
        when(store.id()).thenReturn("store_" + UUID.randomUUID().toString().replaceAll("-", "").substring(0, 7));

        return store;
    }

}
