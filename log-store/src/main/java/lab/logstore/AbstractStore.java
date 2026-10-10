package lab.logstore;

import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractStore implements Store {
    
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
        // Begin warm-up
        for (int i = 0; i < 100000; i++) {
            this.read("key-" + i);
        }
        // End warm-up

        long[] readMeasurements = new long[100000];

        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            this.read("key-" + i);

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
            write("key-" + i, "value-" + i);
        }

        clear();
        // End warm-up

        writeMeasurements = new long[100000];

        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            write("key-" + i, "value-" + i);

            writeMeasurements[i] = (System.nanoTime() - startTime);
        }

        Arrays.sort(writeMeasurements);

        int median = writeMeasurements.length / 2;
        int nintyNineth = (int) (writeMeasurements.length * 0.99);

        return new OperationBenchmark(writeMeasurements[median],
            writeMeasurements[nintyNineth]);
    }

}
