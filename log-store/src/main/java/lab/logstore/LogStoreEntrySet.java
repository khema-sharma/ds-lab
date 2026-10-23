package lab.logstore;

public record LogStoreEntrySet(long offset, long length, LogStoreEntry entry) {
}