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

}