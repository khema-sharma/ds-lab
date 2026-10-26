package lab.logstore;

public sealed interface LogStoreEntry permits DataEntry, CdcEntry {

    public static final byte TYPE_DATA_ENTRY = 1;
    public static final byte TYPE_CDC_ENTRY = 2;

    public static boolean isValidEntryType(byte type) {
        return switch (type) {
            case TYPE_CDC_ENTRY, TYPE_DATA_ENTRY -> true;
            default -> false;
        };
    }

    byte type();
    int length();
    long checksum();
    LogStoreEntryPayload record();

}
