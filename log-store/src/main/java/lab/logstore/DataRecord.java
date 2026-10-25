package lab.logstore;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * 
 * Represents a data record, which is a key-value pair.
 * 
 * @param key
 * @param value
 */
public record DataRecord(String key, String value) implements LogStoreEntryPayload {

    public static Optional<DataRecord> createFromPayload(byte[] recordPayload) {
        try {

            String key = null;
            String value = null;

            ByteBuffer recordBuffer = ByteBuffer.wrap(recordPayload);

            byte[] keyBytes = new byte[recordBuffer.getInt()];
            recordBuffer.get(keyBytes);

            key = new String(keyBytes, "utf-8");

            byte[] valueBytes = new byte[recordBuffer.getInt()];
            recordBuffer.get(valueBytes);

            value = new String(valueBytes, "utf-8");

            return Optional.of(new DataRecord(key, value));

        } catch (UnsupportedEncodingException e) {
        }

        return Optional.empty();
    }

    @Override
    public byte[] getPayload() throws IOException {
        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = value.getBytes();

        return ByteBuffer
                .allocate(Integer.BYTES + keyBytes.length + Integer.BYTES + valueBytes.length)
                .putInt(keyBytes.length)
                .put(keyBytes)
                .putInt(valueBytes.length)
                .put(valueBytes)
                .array();
    }

}