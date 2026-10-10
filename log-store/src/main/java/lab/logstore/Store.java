package lab.logstore;

import java.io.IOException;

public interface Store {
    /**
     * Saves given value in the store and associates the given key to it for later
     * retrieval.
     * 
     * @param key   Key using which the value can be retrieved
     * @param value Value to saved in the store
     * @throws IOException If there were any issues in saving given value in the
     *                     store.
     */
    public void write(String key, String value) throws IOException;

    /**
     * Returns the value associated with the key, <code>null</code> if no value was
     * associated
     * or if <code>null</code> was associated.
     * 
     * @param key Key with which a value is assocoiated
     * @return Value associated with key, can be <code>null</null>
     * @throws IOException
     */
    public String read(String key) throws IOException;

    /**
     * Clear all entries from the store.
     * 
     * @throws IOException if there were issues clearing the store.
     */
    public void clear() throws IOException;

    /**
     * Benchmarks write and read operations.
     * 
     * @return
     */
    public OperationBenchmark[] benchmarkOperations() throws IOException;

}
