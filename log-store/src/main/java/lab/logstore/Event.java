package lab.logstore;

public record Event(int eventId, String nodeId) {

    public Event {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId cannot be blank.");
        }
    }

    public boolean isFromSameNodeAs(Event otherEvent) {
        return (nodeId.equals(otherEvent.nodeId));
    }
    
}