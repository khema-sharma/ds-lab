package lab.ds.logstore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StabilityTest {

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
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
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
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));
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
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
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
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalWaitUntilStateSwitchThreshold();

            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
        }
    }

    private AppendOnlyLogStore mockStore() {
        AppendOnlyLogStore store = mock(AppendOnlyLogStore.class);
        when(store.id()).thenReturn("store_" + UUID.randomUUID().toString().replaceAll("-", "").substring(0, 7));

        return store;
    }

}
