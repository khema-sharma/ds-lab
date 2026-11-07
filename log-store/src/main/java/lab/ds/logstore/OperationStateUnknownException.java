package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class OperationStateUnknownException extends RuntimeException {

    public OperationStateUnknownException(IdempotencyKey key) {
    }

}
