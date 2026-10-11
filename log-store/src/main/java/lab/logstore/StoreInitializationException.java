package lab.logstore;

public class StoreInitializationException extends Exception {
    public StoreInitializationException(Throwable cause) {
        super(cause);
    }

    public StoreInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
