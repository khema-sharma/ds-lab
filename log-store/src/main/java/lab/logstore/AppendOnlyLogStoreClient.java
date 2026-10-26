package lab.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public final class AppendOnlyLogStoreClient implements LogStoreClient, Closeable {

    private final ArrayList<AppendOnlyLogStore> quorumNodes;
    private final int quorumSize;

    public AppendOnlyLogStoreClient(int qSize) throws StoreInitializationException, IOException {
        if (qSize <= 0) {
            throw new IllegalArgumentException("Quorum size must be atleast 1.");
        }

        quorumSize = qSize;
        quorumNodes = new ArrayList<>(qSize);

        initQuorumNodes();
    }

    @Override
    public void put(String key, String value) throws IOException {
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();

        quorumNodes.stream().parallel().forEach(store -> {
            try {
                if (counter.incrementAndGet() < quorumSize) {
                    store.put(key, value);
                    successes.incrementAndGet();
                }

            } catch (IOException e) {
                System.err.println("An error occurred while saving data: " + e.getMessage());
            }
        });

        if (successes.get() < quorumSize - 1) {
            throw new IOException("Error saving data.");
        }
    }

    @Override
    public String get(String key) throws IOException {
        final ArrayList<String> valueVersions = new ArrayList<>(3);
        AtomicInteger counter = new AtomicInteger();

        quorumNodes.stream().parallel().forEach(store -> {
            try {
                if (counter.incrementAndGet() < quorumSize) {
                    valueVersions.add(store.get(key)); 
                }
            } catch (IOException e) {
                System.err.println("An error occurred while retrieving data: " + e.getMessage());
            }
        });

        if (valueVersions.size() < quorumSize - 1) {
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
