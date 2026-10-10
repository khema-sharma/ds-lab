package lab.logstore;

import java.util.LinkedList;
import java.util.List;

public class LamportClock {

    private int counter = 0;

    private List<String> communicatedEvents = new LinkedList<String>();

    public int next() {
        return ++counter;
    }

    public void onEvent(Event event) {
        if (event.eventId() > counter) {
            counter = event.eventId();
            communicatedEvents.add(event.nodeId() + ':' + event.eventId());
        }
    }

    public boolean hasEvent(Event e) {
        return communicatedEvents.contains(e.nodeId() + ':' + e.eventId());
    }
    
}