package lab.logstore;

public record Event(int eventId, String nodeId) {
    public Event {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId cannot be blank.");
        }
    }

    public boolean happenedAfter(Event other) {
        if (other == null) {
            throw new IllegalArgumentException("Other cannot be null.");
        }

        return eventId > other.eventId;
    }

    public boolean happenedBefore(Event other) {
        if (other == null) {
            throw new IllegalArgumentException("Other cannot be null.");
        }

        return eventId < other.eventId;
    }
    
}