package lab.ds.logstore;

import java.util.ArrayList;
import java.util.List;

import lab.ds.logstore.core.IdempotencyKey;

public class QuorumNotReachedException extends RetryableStoreException {

    private final List<Throwable> causes = new ArrayList<>();

    public QuorumNotReachedException(IdempotencyKey key, String message, List<Throwable> causes) {
        super(key, message);
        
        if (causes != null && !causes.isEmpty()) {
            this.causes.addAll(causes);
        }
    }

}
