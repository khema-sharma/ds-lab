package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public abstract class RandomLogStoreAccess extends RandomAccessFile {

    private final Checksum crc32Checksum;

    public RandomLogStoreAccess(File file, String mode) throws FileNotFoundException {
        super(file, mode);
        crc32Checksum = new CRC32();
    }

    public long pos() throws IOException {
        return getFilePointer();
    }

    public boolean isEOF() throws IOException {
        return (pos() == length());
    }

    protected long getChecksum(byte[] contents) {
        crc32Checksum.reset();
        crc32Checksum.update(contents);

        return crc32Checksum.getValue();
    }

}
