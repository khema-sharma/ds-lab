package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class LamportTest {

    @Test
    public void acceptsOrderedEvents() {
        DistributedProcess nodeA = new DistributedProcess("node-a");
        DistributedProcess nodeB = new DistributedProcess("node-b");

        Event nodeAEvent = nodeA.generateEvent();
        nodeB.onEvent(nodeAEvent);
        
        assertTrue(nodeB.generateEvent().happenedAfter(nodeAEvent));
    }

    @Test
    public void acceptsConcurrentEvents() {
        DistributedProcess nodeA = new DistributedProcess("node-a");
        DistributedProcess nodeB = new DistributedProcess("node-b");

        Event nodeAEvent = nodeA.generateEvent();
        Event nodeBEvent = nodeB.generateEvent();

        assertFalse(nodeAEvent.happenedBefore(nodeBEvent));
        assertFalse(nodeBEvent.happenedBefore(nodeAEvent));
    }

}