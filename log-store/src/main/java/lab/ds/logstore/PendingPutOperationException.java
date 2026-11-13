package lab.ds.logstore;

import java.io.IOException;

public class PendingPutOperationException extends IOException {

    public PendingPutOperationException(String message) {
        super(message);
    }

}
