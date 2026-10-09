package lab.logstore;

import java.text.MessageFormat;

public record StoreBenchmark(long readMedianTime, long readNintyNinethPercentileTime, long writeMedianTime,
        long writeNintyNinethTime) {
    public String toString() {
        return MessageFormat.format("Read - Median: {0}, 99P: {1}\nWrite - Median {2}, 99P: {3}",
                readMedianTime, readNintyNinethPercentileTime, writeMedianTime, writeNintyNinethTime);
    }
}
