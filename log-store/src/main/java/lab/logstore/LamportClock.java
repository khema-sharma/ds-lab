package lab.logstore;

public class LamportClock implements Comparable<LamportClock> {

    private int counter = 0;

    public int next() {
        return ++counter;
    }

    public void onEvent(Event event) {
        if (event.eventId() > counter) {
            counter = event.eventId();
        }
    }

    @Override
    public int compareTo(LamportClock o) {
        LamportClock other = (LamportClock) o;

        if (other == this) {
            return 0;
        }

        if (counter < other.counter) {
            return -1;
        } else if (counter > other.counter) {
            return 1;
        }

        return 0;
    }
    
}