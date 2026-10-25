package lab.logstore;

import java.io.IOException;

/**
 */
public sealed interface LogStoreEntryPayload permits LogStoreRecord, LogStoreCDCRecord {
    public byte[] getPayload() throws IOException;
}
