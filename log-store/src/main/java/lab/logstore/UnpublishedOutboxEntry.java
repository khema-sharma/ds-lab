package lab.logstore;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public record UnpublishedOutboxEntry(long id, long offset) {

    static int BYTES = (Long.BYTES * 2);

    public static UnpublishedOutboxEntry createFromInput(DataInput input) throws IOException {
        return new UnpublishedOutboxEntry(input.readLong(), input.readLong());
    }

    void writeEntry(ByteBuffer buffer) {
        buffer.putLong(id);
        buffer.putLong(offset);
    }

}
