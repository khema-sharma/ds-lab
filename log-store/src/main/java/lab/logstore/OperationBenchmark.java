package lab.logstore;

import java.text.MessageFormat;

public record OperationBenchmark(long meanTime, long nintyNinethPercentileTime) {
    public String toString() {
        return MessageFormat.format("Median: {0} ns, 99P: {1} ns",
            meanTime, nintyNinethPercentileTime);
    }
}
