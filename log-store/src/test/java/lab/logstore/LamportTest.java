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

        Event nodeBEvent = nodeB.generateEvent();
        
        assertTrue(nodeB.hasHappenedAfterRelation(nodeBEvent, nodeAEvent));
    }

    @Test
    public void acceptsConcurrentEvents() {
        DistributedProcess nodeA = new DistributedProcess("node-a");
        DistributedProcess nodeB = new DistributedProcess("node-b");

        nodeA.generateEvent();
        Event nodeAEvent = nodeA.generateEvent();

        Event nodeBEvent = nodeB.generateEvent();

        assertFalse(nodeB.hasHappenedBeforeRelation(nodeAEvent, nodeBEvent));
        assertFalse(nodeA.hasHappenedBeforeRelation(nodeBEvent, nodeAEvent));
    }

}