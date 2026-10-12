package lab.logstore;

import java.text.MessageFormat;

public record LogStoreEntry(long offset, int length, long checksum, LogStoreRecord record) {
    public static final String RECORD_ENTRY_SEP = ",";
    public static final String RECORD_ENTRY_PATTERN = "{0}" + RECORD_ENTRY_SEP + "{1}" + RECORD_ENTRY_SEP + "{2}";

    public String toString() {
        return formatStoreEntry();
    }

    private String formatStoreEntry() {
        return MessageFormat.format(RECORD_ENTRY_PATTERN, Integer.toString(length), Long.toString(checksum),
                record.toString());
    }
}
