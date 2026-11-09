package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class QuorumNotReachedException extends RetryableStoreException {

    public QuorumNotReachedException(IdempotencyKey key, String message) {
        super(key, message);
    }

}
