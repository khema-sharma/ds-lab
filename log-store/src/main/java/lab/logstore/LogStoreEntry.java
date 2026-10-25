package lab.logstore;

public record LogStoreEntry<T extends LogStoreEntryPayload>(byte type, int length, long checksum, T record) {

    public static final byte TYPE_LOG_ENTRY = 1;
    public static final byte TYPE_CDC_ENTRY = 2;

    public static boolean isValidEntryType(byte type) {
        return switch (type) {
            case TYPE_CDC_ENTRY, TYPE_LOG_ENTRY -> true;
            default -> false;
        };
    }

    public boolean isLogEntry() {
        return (type == TYPE_LOG_ENTRY);
    }

    public boolean isCDCEntry() {
        return (type == TYPE_CDC_ENTRY);
    }

}