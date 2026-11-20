package lab.ds.logstore;

import java.util.List;

import lab.ds.logstore.core.IdempotencyKey;

public class QuorumNotReachedException extends RetryableStoreException {

    public QuorumNotReachedException(IdempotencyKey key, String message, List<Throwable> causes) {
        super(key, message);

        if (causes != null) {
            causes.forEach(this::addSuppressed);
        }
    }

}
