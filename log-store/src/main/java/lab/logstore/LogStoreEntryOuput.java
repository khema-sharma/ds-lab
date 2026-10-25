package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class LogStoreEntryOuput extends RandomLogStoreAccess {

    public static final int RECORD_OFFSET = (Byte.BYTES + Integer.BYTES + Long.BYTES);

    public LogStoreEntryOuput(File file) throws FileNotFoundException {
        super(file, "rwd");
    }

    public LogStoreEntry putEntry(byte type, LogStoreEntryPayload payload) throws IOException {
        byte[] data = payload.getPayload();
        long checksum = getChecksum(data);

        write(ByteBuffer
                .allocate(RECORD_OFFSET + data.length)
                .put(type)
                .putInt(data.length)
                .putLong(checksum)
                .put(data)
                .array());

        return switch (type) {
            case LogStoreEntry.TYPE_DATA_ENTRY -> new DataEntry(data.length, checksum, (DataRecord) payload);
            case LogStoreEntry.TYPE_CDC_ENTRY -> new CdcEntry(data.length, checksum, (CdcRecord) payload);
            default -> throw new IllegalStateException();
        };
    }

}