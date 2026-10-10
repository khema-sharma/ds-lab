package lab.logstore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InMemoryStore extends AbstractStore {

    public static final String ERR_ILLEGAL_KEY = "Key cannot be blank.";

    private Map<String, String> store = new HashMap<>();
    
    public void write(String key, String value) throws IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException(ERR_ILLEGAL_KEY);
        }

        store.put(key, value);
    }

    @Override
    public String read(String key) throws IOException {
        return store.get(key);
    }

    @Override
    public void clear() throws IOException {
        store.clear();
    }

    public static void main(String[] args) {
        try {
            OperationBenchmark[] benchMarks = new InMemoryStore().benchmarkOperations();

            System.out.println("Write benchmarks: " + benchMarks[1]);
            System.out.println("Read benchmarks: " + benchMarks[0]);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}