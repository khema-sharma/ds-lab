package lab.logstore;

/**
 * 
 * Represents a log store entry at offset {@code offset}, and of length {@code length}.
 * 
 * @param offset Offset at which log store entry can be found
 * @param length Length of the entry
 * @param entry Log store entry
 */
public record LogStoreEntrySet(long offset, long length, LogStoreEntry entry) {
}