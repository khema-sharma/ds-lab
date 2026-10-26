package lab.logstore;

import java.io.IOException;

/**
 * LogStoreClient transparently connects to a {@link Store} and performs
 * reliable {@code put} and {@code get} operations.
 */
public interface LogStoreClient {

    /**
     * 
     * @param key
     * @param value
     */
    public void put(String key, String value) throws IOException;

    /**
     * 
     * @param key
     * @return
     */
    public String get(String key) throws IOException;

}
