package lab.logstore;

import java.io.IOException;

/**
 * A key-value (KV) store.
 */
public interface Store {
    /**
     * Saves given value in the store and associates the given key to it for later
     * retrieval.
     * 
     * @param key   Key using which the value can be retrieved
     * @param value Value of the key to be saved in the store
     * @throws IOException If there were any issues in saving given key-value pair.
     */
    public void put(String key, String value) throws IOException;

    /**
     * Performs an idempotent save of the given value.
     * <p>
     * Repeatadly calling key-value pair with the same {@code idempotencyKey} will
     * not result in new save operations.
     * 
     * @param idempotencyKey Idempotency key associated with this operation
     * @param key Key using which value can be retrieved
     * @param value Value of the key to be saved in the store
     * @return Timestamp of this save operation. Same timestamp gets returned when this method is called with the same {@code idempotencyKey}.
     * @throws IOException If there were any issues in saving given key-value pair
     */
    public long put(IdempotencyKey idempotencyKey, String key, String value) throws IOException;

    /**
     * Returns the value associated with the key, <code>null</code> if no value was
     * associated
     * or if <code>null</code> was associated.
     * 
     * @param key Key with which a value is assocoiated
     * @return Value associated with key, can be <code>null</null>
     * @throws IOException
     */
    public String get(String key) throws IOException;

    /**
     * Clear all entries from the store.
     * 
     * @throws IOException if there were issues clearing the store.
     */
    public void clear() throws IOException;

}
