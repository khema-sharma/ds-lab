package lab.logstore;

import java.io.IOException;

/**
 */
public sealed interface LogStoreEntryPayload permits DataRecord, CdcRecord {
    public byte[] getPayload() throws IOException;
}
