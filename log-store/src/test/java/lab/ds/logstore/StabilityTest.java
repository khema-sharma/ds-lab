package lab.ds.logstore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import lab.ds.logstore.core.IdempotencyKey;

@ExtendWith(MockitoExtension.class)
public class StabilityTest {

    private final int poolSize = 3;

    @Test
    public void aceeptsSuccessfulQuorum() throws IOException, StoreInitializationException {
        int quorumSize = getQuorumSize(poolSize);

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];
        int index = 0;

        for (int i = 0; i < (poolSize - quorumSize); i++) {
            AppendOnlyLogStore firstMockedStore = mockStore();

            doThrow(IOException.class)
                    .when(firstMockedStore)
                    .put(any(), anyString(), anyString());

            stores[index++] = firstMockedStore;
        }

        for (int i = 0; i < quorumSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> 1L)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[index++] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void rejectsUnsuccessfulQuorum() throws IOException, StoreInitializationException {
        int quorumSize = getQuorumSize(poolSize);

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];
        int index = 0;

        for (int i = 0; i < (poolSize - quorumSize); i++) {
            AppendOnlyLogStore quorumMockStore = mockStore();

            doAnswer(invocation -> 1L)
                    .when(quorumMockStore)
                    .put(any(), anyString(), anyString());

            stores[index++] = quorumMockStore;
        }

        for (int i = 0; i < quorumSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[index++] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void rejectsQuorumWhenCircuitTrips() throws IOException, StoreInitializationException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];

        AtomicInteger attemptsCounter1 = new AtomicInteger(0);
        AtomicInteger attemptsCounter2 = new AtomicInteger(0);

        for (int i = 0; i < poolSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .doAnswer(invocation -> 1L)
                    .doAnswer(invocation -> {
                        attemptsCounter1.incrementAndGet();
                        throw new IOException();
                    })
                    .doAnswer(invocation -> {
                        attemptsCounter1.incrementAndGet();
                        throw new IOException();
                    })
                    .doAnswer(invocation -> {
                        attemptsCounter1.incrementAndGet();
                        throw new IOException();
                    })
                    .doAnswer(invocation -> {
                        attemptsCounter2.incrementAndGet();
                        throw new IOException();
                    })
                    .when(mockedStore).put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));

            // Quorum fails
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
            // 3 attemps by each node, so total 9 attempts (starts from 0)
            assertEquals(8, attemptsCounter1.get());

            // Quorum rejected
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));
            assertEquals(0, attemptsCounter2.get());
        }
    }

    @Test
    public void acceptsSuccessfulQuorumAfterProbe()
            throws IOException, StoreInitializationException, InterruptedException {

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];
        int index = 0;

        for (int i = 0; i < poolSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .doThrow(IOException.class)
                    // Quorum not reached -> circuit opens
                    .doThrow(IOException.class)
                    // Answer probes
                    .doAnswer(invocation -> 1L)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[index++] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));
            // Wait until circuit resets, open -> half-open
            storeClient.internalWaitUntilStateSwitchThreshold();
            // half-open -> probe -> closed
            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void rejectsCloseToOpenedStateTransition() throws IOException, StoreInitializationException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];

        for (int i = 0; i < poolSize; i++) {
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
    public void rejectsOpenedToOpenedStateTransition()
            throws IOException, StoreInitializationException, InterruptedException {

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];

        for (int i = 0; i < poolSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .doThrow(IOException.class)
                    // Quorum not reached: closed -> open
                    .doThrow(IOException.class)
                    // Wait until circuit trips
                    // open -> half-open
                    // Store is busy: hal-open -> probing -> open
                    .doThrow(IOException.class)
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            storeClient.internalWaitUntilStateSwitchThreshold();

            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void rejectsSlowQuorum() throws IOException, StoreInitializationException, InterruptedException {
        int quorumSize = getQuorumSize(poolSize);

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];
        int index = 0;

        for (int i = 0; i < quorumSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> {
                Thread.sleep(Duration.ofSeconds(2));
                return invocation.callRealMethod();
            }).when(mockedStore).put(any(), anyString(), anyString());

            stores[index++] = mockedStore;
        }

        for (int i = 0; i < (poolSize - quorumSize); i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> 1L).when(mockedStore).put(any(), anyString(), anyString());
            stores[index++] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = spy(new AppendOnlyLogStoreClient(stores))) {
            assertThrows(OperationStateUnknownException.class, () -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void acceptsSlowQuorumThatCompletes()
            throws IOException, StoreInitializationException, InterruptedException {
        int quorumSize = getQuorumSize(poolSize);

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];
        int index = 0;

        for (int i = 0; i < quorumSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> {
                Thread.sleep(800);
                return 1L;
            }).when(mockedStore).put(any(), anyString(), anyString());

            stores[index++] = mockedStore;
        }

        for (int i = 0; i < (poolSize - quorumSize); i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .when(mockedStore).put(any(), anyString(), anyString());

            stores[index++] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void acceptsSlowQuorumProbesWhichComplete()
            throws IOException, StoreInitializationException, InterruptedException {

        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];

        // All three nodes fail consecutively for 3 times (for each retry)
        // and then each node provides a delayed response
        for (int i = 0; i < poolSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(IOException.class)
                    .doThrow(IOException.class)
                    // Quorum not reached -> circuit opens
                    .doThrow(IOException.class)

                    // Answers probe with a delay which finishes within quorum timeout
                    .doAnswer(invocation -> {
                        Thread.sleep(Duration.ofMillis(800));
                        return 1L;
                    })
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
            // Wait until circuit resets, open -> half-open
            storeClient.internalWaitUntilStateSwitchThreshold();
            // half-open -> probe -> closed
            assertDoesNotThrow(() -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void rejectsSlowQuorumProbes() throws IOException, StoreInitializationException, InterruptedException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];

        // All three nodes fail consecutively for 3 times (for each retry)
        // and then each node provides a delayed response
        for (int i = 0; i < poolSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doThrow(new IOException("First exception from mock " + i))
                    .doThrow(new IOException("Second exception from mock " + i))
                    // Quorum not reached -> circuit opens
                    .doThrow(new IOException("Third exception from mock " + i))
                    // Answers probe with a delay which results in quorum timeout
                    .doAnswer(invocation -> {
                        Thread.sleep(Duration.ofSeconds(2));
                        return 1L;
                    })
                    .when(mockedStore)
                    .put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put("key-1", "value-1"));
            // Wait until circuit resets, open -> half-open
            storeClient.internalWaitUntilStateSwitchThreshold();
            // half-open -> probe -> closed
            assertThrows(OperationStateUnknownException.class, () -> storeClient.put("key-1", "value-1"));
        }
    }

    @Test
    public void acceptsCallerSideErrorsDoNotTripCircuit() throws IOException, StoreInitializationException {
        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(poolSize)) {
            IdempotencyKey idempotencyKey = new IdempotencyKey("id");

            IdempotencyKey spyKey = spy(idempotencyKey);
            when(spyKey.key()).then(invocation -> null);

            // An invalid input must fail, but not trip the circuit
            assertThrows(QuorumNotReachedException.class, () -> storeClient.put(spyKey, "key-1", "value-1"));

            // A valid input must pass through
            assertDoesNotThrow(() -> storeClient.put(idempotencyKey, "key-1", "value-1"));
        }
    }

    private AppendOnlyLogStore mockStore() {
        AppendOnlyLogStore store = mock(AppendOnlyLogStore.class);
        when(store.id()).thenReturn("store_" + UUID.randomUUID().toString().replaceAll("-", "").substring(0, 7));

        return store;
    }

    private int getQuorumSize(int poolSize) {
        return (poolSize * 4) / 5;
    }

}
