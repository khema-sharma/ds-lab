package lab.ds.logstore;

import java.io.IOException;

import lab.ds.logstore.core.IdempotencyKey;
import lab.ds.logstore.core.Store;

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
     * @param idempotencyKey
     * @param key
     * @param value
     * @throws IOException
     */
    public void put(IdempotencyKey idempotencyKey, String key, String value) throws IOException;

    /**
     * 
     * @param key
     * @return
     */
    public String get(String key) throws IOException;

}
