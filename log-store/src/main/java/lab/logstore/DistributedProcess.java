package lab.logstore;

public class DistributedProcess {

    private LamportClock nodeClock = new LamportClock();
    private String name = "";

    public DistributedProcess(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be blank.");
        }

        this.name = name;
    }

    public Event generateEvent() {
        return new Event(nodeClock.next(), name);
    }

    public void onEvent(Event event) {
        nodeClock.onEvent(event);
    }

    public boolean hasHappenedAfterRelation(Event nodeEvent, Event otherEvent) {
        if (nodeClock.hasEvent(otherEvent)
            || otherEvent.isFromSameNodeAs(nodeEvent)) {
            return nodeEvent.eventId() > otherEvent.eventId();
        }

        return false;
    }

    public boolean hasHappenedBeforeRelation(Event otherEvent, Event nodeEvent) {
        if (nodeClock.hasEvent(otherEvent)
            || otherEvent.isFromSameNodeAs(nodeEvent)) {
            return otherEvent.eventId() < nodeEvent.eventId();
        }

        return false;
    }

}