package lab.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class RandomLogStoreAccess extends RandomAccessFile {

    public RandomLogStoreAccess(File file, String mode) throws FileNotFoundException {
        super(file, mode);
    }

    public long pos() throws IOException {
        return getFilePointer();
    }

}
