package lab.logstore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InMemoryLogStore implements LogStore {

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
            System.out.println(new InMemoryLogStore().benchmark());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}