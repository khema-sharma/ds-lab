package lab.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyLogStoreClient.class);

    private final ArrayList<AppendOnlyLogStore> quorumNodes;
    private final int quorumSize;

    private final ExecutorService quorumExecutorService;
    private final ScheduledExecutorService bgExecutorService;

    private final AtomicInteger circuitBreakerCount;
    private final AtomicReference<CountDownLatch> circuitStateSwitchLatch;

    public AppendOnlyLogStoreClient(int qSize) throws StoreInitializationException, IOException {
        this(qSize, null);
    }

    AppendOnlyLogStoreClient(AppendOnlyLogStore[] quorum) throws StoreInitializationException, IOException {
        this(-1, quorum);
    }

    private AppendOnlyLogStoreClient(int capacity, AppendOnlyLogStore[] quorum) throws StoreInitializationException {
        int qSize = (quorum != null ? quorum.length : capacity);

        if (qSize <= 2) {
            throw new IllegalArgumentException("Quorum size must be atleast 3.");
        }

        quorumSize = qSize;
        quorumNodes = new ArrayList<>(quorumSize);

        quorumExecutorService = Executors.newFixedThreadPool(5);
        bgExecutorService = Executors.newScheduledThreadPool(1);

        circuitBreakerCount = new AtomicInteger(0);
        circuitStateSwitchLatch = new AtomicReference<>(new CountDownLatch(0));

        for (int i = 0; i < quorumSize; i++) {
            quorumNodes.add(quorum == null ? new AppendOnlyLogStore() : quorum[i]);
        }
    }

    @Override
    public void put(String key, String value) throws IOException {
        stopIfCircuitIsOpen();

        CountDownLatch quorumLatch = new CountDownLatch(quorumSize - 1);
        List<Throwable> errors = new ArrayList<>(quorumSize);

        quorumNodes.stream().forEach(store -> {
            quorumExecutorService.submit(() -> {
                try {
                    store.put(key, value);

                } catch (IOException e) {
                    errors.add(e);

                } finally {
                    quorumLatch.countDown();
                }
            });
        });

        try {
            quorumLatch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Quorum write latch interrupted!");
        }

        boolean didNotHearFromQuorum = (quorumLatch.getCount() > 0);
        boolean experiencedErrors = !errors.isEmpty();

        if (didNotHearFromQuorum || experiencedErrors) {
            tripCircuitOnThresholdBreach();

            if (didNotHearFromQuorum) {
                throw new StoreOperationTimedOutException();
            }

            throw new IOException("Error saving data.");
        }

        if (circuitBreakerCount.get() > 0) {
            logger.info("Resetting circuit to open state ...");
            circuitBreakerCount.setRelease(0);
        }
    }

    @Override
    public String get(String key) throws IOException {
        final List<String> valueVersions = Collections.synchronizedList(new ArrayList<>(3));
        CountDownLatch quorumLatch = new CountDownLatch(quorumSize - 1);

        quorumNodes.stream().forEach(store -> {
            quorumExecutorService.submit(() -> {
                try {
                    valueVersions.add(store.get(key));
                    quorumLatch.countDown();
                } catch (IOException e) {
                    System.err.println("An error occurred while retrieving data: " + e.getMessage());
                }
            });
        });

        try {
            quorumLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Quorum read latch interrupted.");
        }

        if (quorumLatch.getCount() > 0) {
            throw new IOException("Error retrieving data.");
        }

        return valueVersions.get(0);
    }

    @Override
    public void close() throws IOException {
        quorumNodes.forEach(store -> {
            try {
                store.close();
            } catch (IOException e) {
                System.err.println("An error occurred while closing the store: " + e.getMessage());
            }
        });
    }

    void internalBlockUntilCircuitIsOpen() throws InterruptedException {
        circuitStateSwitchLatch.get().await(5, TimeUnit.SECONDS);
    }

    private void tripCircuitOnThresholdBreach() {
        synchronized (this) {
            circuitBreakerCount.incrementAndGet();

            if (circuitBreakerCount.get() == 3) {
                logger.warn("Tripping circuit and scheduling backend task to reset automatically after 3 seconds.");

                circuitStateSwitchLatch.set(new CountDownLatch(1));

                bgExecutorService.schedule(() -> {
                    logger.info("Resetting circuit to half-open state ...");

                    circuitBreakerCount.setRelease(2);
                    circuitStateSwitchLatch.get().countDown();
                }, 3, TimeUnit.SECONDS);
            }
        }
    }

    private void stopIfCircuitIsOpen() {
        if (circuitBreakerCount.get() >= 3) {
            logger.warn("Open circuit, won't invoke integration endpoint!");
            throw new StoreBusyException();
        }
    }

}
