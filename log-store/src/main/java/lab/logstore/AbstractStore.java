package lab.logstore;

import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractStore implements Store {
    
    public long put(IdempotencyKey idempotencyKey, String key, String value) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Benchmarks by writing and reading 100000 entries.
     * 
     * @return StoreBenchmark capturing median and 99th percentile times.
     */
    public OperationBenchmark[] benchmarkOperations() throws IOException {
        return new OperationBenchmark[] { benchmarkWriteOperations(),
            benchmarkReadOperations() };      
    }

    private OperationBenchmark benchmarkReadOperations() throws IOException {
        clear();

        // Populate records
        for (int i = 0; i < 100000; i++) {
            put("key-" + i, "value-" + i);
        }

        // Begin warm-up
        for (int i = 0; i < 100000; i++) {
            this.get("key-" + i);
        }
        // End warm-up

        long[] readMeasurements = new long[100000];

        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            this.get("key-" + i);

            readMeasurements[i] = (System.nanoTime() - startTime);
        }

        Arrays.sort(readMeasurements);

        int median = readMeasurements.length / 2;
        int nintyNineth = (int) (readMeasurements.length * 0.99);

        return new OperationBenchmark(readMeasurements[median],
            readMeasurements[nintyNineth]);
    }

    private OperationBenchmark benchmarkWriteOperations() throws IOException {
        long[] writeMeasurements = new long[100000];

        // Begin warm-up
        for (int i = 0; i < 100000; i++) {
            put("key-" + i, "value-" + i);
        }

        clear();
        // End warm-up

        writeMeasurements = new long[100000];

        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            put("key-" + i, "value-" + i);

            writeMeasurements[i] = (System.nanoTime() - startTime);
        }

        Arrays.sort(writeMeasurements);

        int median = writeMeasurements.length / 2;
        int nintyNineth = (int) (writeMeasurements.length * 0.99);

        return new OperationBenchmark(writeMeasurements[median],
            writeMeasurements[nintyNineth]);
    }

}
