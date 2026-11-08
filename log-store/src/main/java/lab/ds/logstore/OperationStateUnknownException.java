package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class OperationStateUnknownException extends RuntimeException {

    public OperationStateUnknownException(IdempotencyKey key, Throwable cause) {
        super(cause.getMessage(), cause);
    }

    public OperationStateUnknownException(IdempotencyKey key, String message) {
        super(message, null);
    }
}
