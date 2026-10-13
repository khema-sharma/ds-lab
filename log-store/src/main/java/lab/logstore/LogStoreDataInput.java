package lab.logstore;

import java.io.IOException;

/**
 * Provides data from an input stream.
 */
public interface LogStoreDataInput {
    
    /**
     * Read a byte.
     * 
     * @return Read byte or -1 if end-of-stream reached
     * @throws IOException
     */
    public int read() throws IOException;

    /**
     * Read array of bytes, up to the provided {@code length}.
     * 
     * 
     * @param length
     * @return
     * @throws IOException
     */
    public byte[] read(int length) throws IOException;

}