package lab.logstore;

/**
 * DataEntry
 */
public final record DataEntry(int length, long checksum, DataRecord record) implements LogStoreEntry {

    @Override
    public byte type() {
        return LogStoreEntry.TYPE_DATA_ENTRY;
    }

}
