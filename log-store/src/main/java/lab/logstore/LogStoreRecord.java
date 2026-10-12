package lab.logstore;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;

public record LogStoreRecord(String key, String value) {
    public static final String KEY_VALUE_SEP = ":";
    public static final String KEY_VALUE_PATTERN = "{0}" + KEY_VALUE_SEP + "{1}";

    public LogStoreRecord {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null.");
        }
    }

    public byte[] getBytes() {
        try {
            return toString().getBytes("utf-8");
            
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    public String toString() {
        return formatKeyValuePair();
    }

    private String formatKeyValuePair() {
        return MessageFormat.format(KEY_VALUE_PATTERN, key, value);
    }
}
