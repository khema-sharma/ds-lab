package lab.logstore;

import java.io.IOException;
import java.util.Arrays;

public interface LogStore {
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
     * Benchmarks by writing and reading 100000 entries.
     * 
     * @return StoreBenchmark capturing median and 99th percentile times.
     */
    public default StoreBenchmark benchmark() throws IOException {
        long[] readMeasurements = new long[100000];
        long[] writeMeasurements = new long[100000];

        // Begin warm-up
        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            this.write("key-" + i, "value-" + i

            );

            writeMeasurements[i] = (System.nanoTime() - startTime);
        }

        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            this.read("key-" + i);

            readMeasurements[i] = (System.nanoTime() - startTime);
        }
        // End warm-up

        clear();

        readMeasurements = new long[100000];
        writeMeasurements = new long[100000];

        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            this.write("key-" + i, "value-" + i

            );

            writeMeasurements[i] = (System.nanoTime() - startTime);
        }

        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            this.read("key-" + i);

            readMeasurements[i] = (System.nanoTime() - startTime);
        }

        Arrays.sort(readMeasurements);
        Arrays.sort(writeMeasurements);

        int median = readMeasurements.length / 2;
        int nintyNineth = (int) (readMeasurements.length * 0.99);

        return new StoreBenchmark(readMeasurements[median], readMeasurements[nintyNineth], writeMeasurements[median],
                writeMeasurements[nintyNineth]);
    }

}
