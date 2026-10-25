package lab.logstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * 
 * Represents a Change Data Capture (CDC) record.
 * <p>
 * A CDC record captures an {@link #type}, and its data i.e. a {@code key} and
 * its {@code value}.
 * </p>
 * 
 * @param type
 * @param key
 * @param value
 */
public record CdcRecord(byte type, long timestamp, Object event) implements LogStoreEntryPayload {
    public static final byte TYPE_INSERT = 1;
    public static final byte TYPE_UPDATE = 2;
    public static final byte TYPE_DELETE = 3;

    public static Optional<CdcRecord> createFromPayload(byte[] recordPayload) {
        if (recordPayload == null || recordPayload.length == 0) {
            throw new IllegalArgumentException("payload cannot empty.");
        }

        ByteBuffer payloadBuffer = ByteBuffer.wrap(recordPayload);

        try {
            byte type = payloadBuffer.get();
            if (!isValidType(type)) {
                throw new IllegalStateException("Invalid record, unknown event type: " + (int) type);
            }

            long timestamp = payloadBuffer.getLong();
            if (timestamp <= 0) {
                throw new IllegalStateException("Invalid record, timestamp must be > 0.");
            }

            int length = payloadBuffer.remaining();
            Object event = null;

            if (length > 0) {
                byte[] eventPayload = new byte[length];
                payloadBuffer.get(eventPayload);

                try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(eventPayload))) {
                    event = stream.readObject();
                }
            }

            return Optional.of(new CdcRecord(type, timestamp, event));

        } catch (Exception e) {
            System.err.println("Error while deserializing CDC record: " + e.getMessage());
        }

        return Optional.empty();
    }

    public static boolean isValidType(byte type) {
        return switch (type) {
            case TYPE_INSERT, TYPE_UPDATE, TYPE_DELETE -> true;
            default -> false;
        };
    }

    public CdcRecord {
        if (event == null) {
            throw new IllegalArgumentException("event cannot be blank.");
        }

        if (timestamp <= 0) {
            throw new IllegalArgumentException("Invalid timestamp value.");
        }
    }

    @Override 
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        try (ObjectOutputStream stream = new ObjectOutputStream(byteStream)) {
            stream.writeObject(event);
            stream.flush();

            ByteBuffer cdcBuffer = ByteBuffer.allocate(Byte.BYTES + Long.BYTES + byteStream.size());

            cdcBuffer.put(type);
            cdcBuffer.putLong(timestamp);
            cdcBuffer.put(byteStream.toByteArray());

            return cdcBuffer.array();
        }
    }

}
