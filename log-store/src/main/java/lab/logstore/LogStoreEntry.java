package lab.logstore;

public record LogStoreEntry(byte type, int length, long checksum, LogStoreRecord record) {

    public static final byte TYPE_LOG_ENTRY = 1;
    public static final byte TYPE_CDC_ENTRY = 2;

    public static boolean isValidEntryType(byte type) {
        return switch (type) {
            case TYPE_CDC_ENTRY, TYPE_LOG_ENTRY -> true;
            default -> false;
        };
    }

}