package lab.logstore;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Optional;

public record LogStoreRecord(String key, String value) {

    public static final String KEY_VALUE_SEP = ":";
    public static final String KEY_VALUE_PATTERN = "{0}" + KEY_VALUE_SEP + "{1}";

    public static Optional<LogStoreRecord> fromBytes(byte[] bytes) {
        try {

            String keyValuePair = new String(bytes, "utf-8");
            int valueIndex = keyValuePair.indexOf(":");

            String aKey = keyValuePair.substring(0, valueIndex);
            String keyValue = keyValuePair.substring(valueIndex + 1);

            return Optional.of(new LogStoreRecord(aKey, keyValue));

        } catch (UnsupportedEncodingException use) {
            use.printStackTrace();
        }

        return Optional.empty();
    }

    public LogStoreRecord {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null.");
        }
    }

    public String toString() {
        return MessageFormat.format(KEY_VALUE_PATTERN, key, value);
    }
}