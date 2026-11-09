package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class OperationStateUnknownException extends RetryableStoreException {

    public OperationStateUnknownException(IdempotencyKey key, Throwable cause) {
        super(key, cause);
    }

    public OperationStateUnknownException(IdempotencyKey key, String message) {
        super(key, message);
    }
    
}
