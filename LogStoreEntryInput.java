public class LogStoreEntryInput extends RandomAccessFile {
    public LogStoreEntryInput(File file) throws FileNotFoundException {
        super(file, "r");
        crc32Checksum = new CRC32();
    }
}
