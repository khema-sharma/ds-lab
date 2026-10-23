package lab.logstore;

import java.text.MessageFormat;

public record LogStoreEntry(byte type, int length, long checksum, LogStoreRecord record) {

    public static final byte TYPE_LOG_ENTRY = 1;
    public static final byte TYPE_CDC_ENTRY = 2;

    public static final String LOG_ENTRY_SEP = "\n";
    
    public static final String RECORD_ENTRY_SEP = ",";
    public static final String RECORD_ENTRY_PATTERN = "{0}" + RECORD_ENTRY_SEP + "{1}" + RECORD_ENTRY_SEP + "{2}"
            + RECORD_ENTRY_SEP + "{3}";

    public static boolean isValidEntryType(byte type) {
        return switch (type) {
            case TYPE_CDC_ENTRY, TYPE_LOG_ENTRY -> true;
            default -> false;
        };
    }

    public String toString() {
        return formatStoreEntry();
    }

    private String formatStoreEntry() {
        return MessageFormat.format(RECORD_ENTRY_PATTERN, type, Integer.toString(length), Long.toString(checksum),
                record.toString());
    }

}