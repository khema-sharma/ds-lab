package lab.ds.logstore;

import java.io.IOException;

import lab.ds.logstore.core.IdempotencyKey;

/**
 * An {@code IOException} which provides {@code idempotencyKey} so that the
 * caller can retry the operation again (usually {@code put}).
 */
public class RetryableStoreException extends IOException {

    private final IdempotencyKey idempotencyKey;

    public RetryableStoreException(IdempotencyKey key, Throwable cause) {
        super(cause.getMessage(), cause);
        idempotencyKey = key;
    }

    public RetryableStoreException(IdempotencyKey key, String message) {
        super(message, null);
        idempotencyKey = key;
    }

    public IdempotencyKey idempotencyKey() {
        return idempotencyKey;
    }

}
