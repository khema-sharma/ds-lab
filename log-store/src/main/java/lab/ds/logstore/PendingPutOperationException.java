package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class PendingPutOperationException extends RetryableStoreException {

    public PendingPutOperationException(IdempotencyKey idempotencyKey, String message) {
        super(idempotencyKey, message);
    }

}
