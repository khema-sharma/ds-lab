package lab.logstore;

/**
 * CdcEntry
 */
public final record CdcEntry(int length, long checksum, CdcRecord record) implements LogStoreEntry {

    @Override
    public byte type() {
        return LogStoreEntry.TYPE_CDC_ENTRY;
    }

}
