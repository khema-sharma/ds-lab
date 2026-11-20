package lab.ds.logstore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import lab.ds.logstore.AppendOnlyLogStoreClient.ResilientNode;
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

            doThrow(IOException.class).when(mockedStore).put(any(), anyString(), anyString());
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
                    // This resets the failure counter
                    .doAnswer(invocation -> 1L)
                    // Failed - 1
                    .doAnswer(invocation -> {
                        attemptsCounter1.incrementAndGet();
                        throw new IOException();
                    })
                    // Failed - 2
                    .doAnswer(invocation -> {
                        attemptsCounter1.incrementAndGet();
                        throw new IOException();
                    })
                    // Failed - 3
                    // Trips the circuit
                    .doAnswer(invocation -> {
                        attemptsCounter1.incrementAndGet();
                        throw new IOException();
                    })
                    // This must never get answered
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

            // Quorum has attempted MAX_RETRIES attempts on quorum before tripping the
            // circuit
            boolean quorumRetried = (attemptsCounter1.get() >= getQuorumSize(poolSize)
                    * AppendOnlyLogStoreClient.ResilientNode.MAX_RETRIES
                    && attemptsCounter1.get() <= poolSize * AppendOnlyLogStoreClient.ResilientNode.MAX_RETRIES);

            assertEquals(true, quorumRetried);

            // Quorum rejected
            assertThrows(StoreBusyException.class, () -> storeClient.put("key-1", "value-1"));

            // No attempts are made when circuit is tripped
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
                Thread.sleep(Duration.ofSeconds(4));
                return 1L;
            }).when(mockedStore).put(any(), anyString(), anyString());

            stores[index++] = mockedStore;
        }

        for (int i = 0; i < (poolSize - quorumSize); i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> 1L).when(mockedStore).put(any(), anyString(), anyString());
            stores[index++] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
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
                TimeUnit.SECONDS.sleep(2);
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
                        TimeUnit.SECONDS.sleep(2);
                        return 1L;
                    })
                    .doThrow(IOException.class)
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
                        Thread.sleep(Duration.ofSeconds(4));
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
            // half-open -> probe -> open
            assertThrows(OperationStateUnknownException.class, () -> storeClient.put("key-1", "value-1"));
            assertEquals(true, storeClient.allCircuitsInState(ResilientNode.State.Open));
        }
    }

    @Test
    public void acceptsCallerSideErrorsDoNotTripCircuit() throws IOException, StoreInitializationException {
        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(poolSize)) {
            IdempotencyKey idempotencyKey = new IdempotencyKey("id");

            IdempotencyKey spyKey = spy(idempotencyKey);
            when(spyKey.key()).then(invocation -> null);

            // An invalid input must fail, but not trip the circuit
            assertThrows(IllegalArgumentException.class, () -> storeClient.put(spyKey, "key-1", "value-1"));
            assertEquals(true, storeClient.allCircuitsInState(ResilientNode.State.Closed));
        }
    }

    @Test
    public void rejectSlowPutMidLoopTripAsUnknownState()
            throws IOException, StoreInitializationException, InterruptedException, ExecutionException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];

        for (int i = 0; i < poolSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();

            doAnswer(invocation -> {
                Thread.sleep(Duration.ofSeconds(4));
                return 1L;
            }).when(mockedStore).put(any(), anyString(), anyString());

            stores[i] = mockedStore;
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            try (ExecutorService service = Executors.newFixedThreadPool(2)) {
                AtomicReference<Throwable> exception1 = new AtomicReference<>();
                AtomicReference<Throwable> exception2 = new AtomicReference<>();

                Future<?> future1 = service.submit(() -> {
                    try {
                        storeClient.put("key-1", "value-1");
                    } catch (Throwable t) {
                        exception1.set(t);
                    }
                });

                Future<?> future2 = service.submit(() -> {
                    try {
                        storeClient.put("key-2", "value-2");
                    } catch (Throwable t) {
                        exception2.set(t);
                    }
                });

                future1.get();
                future2.get();

                assertInstanceOf(OperationStateUnknownException.class, exception1.get());
                Arrays.stream(exception1.get().getSuppressed())
                        .forEach(t -> assertInstanceOf(OperationStateUnknownException.class, t));

                assertInstanceOf(OperationStateUnknownException.class, exception2.get());
                Arrays.stream(exception2.get().getSuppressed())
                        .forEach(t -> assertInstanceOf(OperationStateUnknownException.class, t));

                assertTrue(storeClient.allCircuitsInState(ResilientNode.State.Open));
            }
        }
    }

    @Test
    public void rejectDefinitiveErrorMidLoopTripAsQuorumNotReached()
            throws IOException, StoreInitializationException, InterruptedException, ExecutionException {
        AppendOnlyLogStore[] stores = new AppendOnlyLogStore[poolSize];

        for (int i = 0; i < poolSize; i++) {
            AppendOnlyLogStore mockedStore = mockStore();
            stores[i] = mockedStore;

            doThrow(IOException.class).when(mockedStore).put(any(), anyString(), anyString());
        }

        try (AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(stores)) {
            try (ExecutorService service = Executors.newFixedThreadPool(2)) {
                AtomicReference<Throwable> exception1 = new AtomicReference<>();
                AtomicReference<Throwable> exception2 = new AtomicReference<>();

                Future<?> future1 = service.submit(() -> {
                    try {
                        storeClient.put("key-1", "value-1");
                    } catch (Throwable t) {
                        exception1.set(t);
                    }
                });

                Future<?> future2 = service.submit(() -> {
                    try {
                        storeClient.put("key-2", "value-2");
                    } catch (Throwable t) {
                        exception2.set(t);
                    }
                });

                future1.get();
                future2.get();

                assertInstanceOf(QuorumNotReachedException.class, exception1.get());
                Arrays.stream(exception1.get().getSuppressed())
                        .forEach(t -> assertInstanceOf(AppendOnlyLogStoreClient.OperationRuntimeException.class,
                                t));

                assertInstanceOf(QuorumNotReachedException.class, exception2.get());
                Arrays.stream(exception2.get().getSuppressed())
                        .forEach(t -> assertInstanceOf(AppendOnlyLogStoreClient.OperationRuntimeException.class,
                                t));

                assertTrue(storeClient.allCircuitsInState(ResilientNode.State.Open));
            }
        }
    }

    @Test
    public void acceptsCleanShutdown() throws IOException, StoreInitializationException {
        AppendOnlyLogStoreClient storeClient = new AppendOnlyLogStoreClient(poolSize);

        storeClient.put("key-1", "value-1");
        storeClient.close();

        assertEquals(true, storeClient.quorumExecutorService.isTerminated());
        storeClient.nodes.forEach(n -> assertTrue(n.putExecutor.isTerminated()));
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
