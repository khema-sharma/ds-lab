package lab.logstore;

import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationState {
    private static final AtomicInteger replicationTerm = new AtomicInteger();

    public static int getNextTerm() {
        return replicationTerm.incrementAndGet();
    }
}
