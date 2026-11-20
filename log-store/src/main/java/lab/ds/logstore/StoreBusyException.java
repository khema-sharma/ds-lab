package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class StoreBusyException extends RetryableStoreException {

    public StoreBusyException(IdempotencyKey key) {
        super(key, "Quorum is unavailable, cannot perform put.");
    }

}
