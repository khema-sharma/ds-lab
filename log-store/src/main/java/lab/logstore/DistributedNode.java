package lab.logstore;

public class DistributedNode {
    
    private LamportClock nodeClock = new LamportClock();
    private String nodeId = "";

    public DistributedNode(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be blank.");
        }

        nodeId = id;
    }

    public Event generateEvent() {
        return new Event(nodeClock.next(), nodeId);
    }

    public void onEvent(Event event) {
        nodeClock.next(event);
    }

}