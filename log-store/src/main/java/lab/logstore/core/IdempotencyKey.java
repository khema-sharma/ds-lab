package lab.logstore.core;

public record IdempotencyKey(String key) {
    public IdempotencyKey {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank.");
        }
    }
}
