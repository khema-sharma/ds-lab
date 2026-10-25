package lab.logstore;

import java.io.Serializable;

/**
 * An event that indicates that a record was inserted in the log store.
 * 
 * @param key Key
 * @param value
 */
public record LogStoreInsertEvent(String key, String value) implements LogStoreEvent, Serializable {
}
