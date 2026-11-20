package lab.ds.logstore;

import java.util.List;

import lab.ds.logstore.core.IdempotencyKey;

public class OperationStateUnknownException extends RetryableStoreException {

    public OperationStateUnknownException(IdempotencyKey key, Throwable cause) {
        super(key, cause);
    }

    public OperationStateUnknownException(IdempotencyKey key, String message) {
        super(key, message);
    }

    public OperationStateUnknownException(IdempotencyKey key, String message, Throwable cause) {
        super(key, message, cause);
    }

    public OperationStateUnknownException(IdempotencyKey key, String message, List<Throwable> causes) {
        super(key, message);
        
        if (causes != null) {
            causes.forEach(this::addSuppressed);
        }
    }

}
