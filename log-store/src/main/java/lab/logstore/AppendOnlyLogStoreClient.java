package lab.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private final ArrayList<AppendOnlyLogStore> quorumNodes;
    private final int quorumSize;

    private final ExecutorService quorumService;

    public AppendOnlyLogStoreClient(int qSize) throws StoreInitializationException, IOException {
        if (qSize <= 0) {
            throw new IllegalArgumentException("Quorum size must be atleast 1.");
        }

        quorumSize = qSize;
        quorumNodes = new ArrayList<>(qSize);

        quorumService = Executors.newFixedThreadPool(5);

        initQuorumNodes();
    }

    @Override
    public void put(String key, String value) throws IOException {
        CountDownLatch quorumLatch = new CountDownLatch(quorumSize - 1);

        quorumNodes.stream().forEach(store -> {
            quorumService.submit(() -> {
                try {
                    store.put(key, value);
                    quorumLatch.countDown();

                } catch (IOException e) {
                    System.err.println("An error occurred while saving data: " + e.getMessage());
                }
            });
        });

        try {
            quorumLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Quorum write latch interrupted.");
        }

        if (quorumLatch.getCount() > 0) {
            throw new IOException("Error saving data.");
        }
    }

    @Override
    public String get(String key) throws IOException {
        final List<String> valueVersions = Collections.synchronizedList(new ArrayList<>(3));
        CountDownLatch quorumLatch = new CountDownLatch(quorumSize - 1);

        quorumNodes.stream().forEach(store -> {
            quorumService.submit(() -> {
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
            throw new IOException("Error saving data.");
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

    private void initQuorumNodes() throws StoreInitializationException, IOException {
        for (int i = 0; i < quorumSize; i++) {
            quorumNodes.add(new AppendOnlyLogStore());
        }
    }

}
