package lab.logstore;

import java.text.MessageFormat;

public class StoreInitializationException extends Exception {
    public StoreInitializationException(Throwable cause) {
        this(MessageFormat.format("An error occurred while initializing store: {0}", cause.getMessage()), cause);
    }

    public StoreInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
