package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class QuorumNotReachedException extends RuntimeException {

    public QuorumNotReachedException(String message, IdempotencyKey key) {
        super(message);
    }

}
