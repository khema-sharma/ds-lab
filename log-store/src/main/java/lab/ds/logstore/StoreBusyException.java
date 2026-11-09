package lab.ds.logstore;

import lab.ds.logstore.core.IdempotencyKey;

public class StoreBusyException extends RuntimeException {

    public StoreBusyException(IdempotencyKey key) {
    }

}
