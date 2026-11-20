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
    public void put(String key, String value) throws RetryableStoreException;

    /**
     * Saves the key-value pair (record) on the quorum
     * 
     * <p>
     * If the put operation is successful then it is guaranteed that the key-value
     * pair has been saved successfully in the nodes which participated in the
     * quorum. There is no guarantee on the remaining nodes, so a read may return a
     * stale value if it reaches a node on which put failed.
     * 
     * <p>
     * If the put operation fails for any reason then the caller must call this
     * method again with the same idempotency key. This ensures that the record gets
     * saved only if it was not previously saved.
     * 
     * @throws RetryableStoreException if there were issues in writing the
     *                                 key-value pair
     */
    public void put(IdempotencyKey idempotencyKey, String key, String value) throws RetryableStoreException;

    /**
     * 
     * @param key
     * @return
     */
    public String get(String key) throws IOException;

}
