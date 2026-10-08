package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class PropertyDefinitionTest {
    private final String scalabilityMetric = "99th percentile for a get, at 100 reads per second, against the target.";
    private final String scalabilityUnit = "milliseconds";
    private final String scalabilityTarget = "200";
    private final String scalabilityFailure = "The get request slower than the target and the caller will time out.";

    private final String reliabilityMetric = "Write failures, at 100 writes per second, against the target.";
    private final String reliabilityUnit = "count";
    private final String reliabilityTarget = "0";
    private final String reliabilityFailure = "The write request rate higher than target and the caller will get error.";

    private final String maintainabilityMetric = "Cyclometric complexity of the code base against the target.";
    private final String maintainabilityUnit = "count";
    private final String maintainabilityTarget = "10";
    private final String maintainabilityFailure = "New team member making changes will see production issues.";

    @Test
    public void rejectPropertyWithBlankMetric() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new PropertyDefinition(Property.SCALABILITY,
                null,
                null,
                null,
                null)
        );

        assertEquals(PropertyDefinition.ERR_ILLEGAL_METRIC, exception.getMessage());
    }

    @Test
    public void rejectPropertyWithBlankMetricUnit() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new PropertyDefinition(Property.SCALABILITY,
                scalabilityMetric,
                null,
                null,
                null)
        );

        assertEquals(PropertyDefinition.ERR_ILLEGAL_UNIT, exception.getMessage());
    }
    
    @Test
    public void rejectPropertyWithBlankTarget() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new PropertyDefinition(Property.SCALABILITY,
                scalabilityMetric,
                scalabilityUnit,
                null,
                null)
        );

        assertEquals(PropertyDefinition.ERR_ILLEGAL_TARGET, exception.getMessage());
    }

    @Test
    public void rejectPropertyWithBlankFailure() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new PropertyDefinition(Property.SCALABILITY,
                scalabilityMetric,
                scalabilityUnit,
                scalabilityTarget, 
                null)
        );

        assertEquals(PropertyDefinition.ERR_ILLEGAL_FAILURE, exception.getMessage());
    }

    @Test
    public void acceptsCompleteDefinition() {
        assertDoesNotThrow(
            () -> new PropertyDefinition(Property.SCALABILITY,
                scalabilityMetric, 
                scalabilityUnit,
                scalabilityTarget,
                scalabilityFailure)
        );
        assertDoesNotThrow(
            () -> new PropertyDefinition(Property.RELIABILITY,
                reliabilityMetric,
                reliabilityUnit,
                reliabilityTarget,
                reliabilityFailure)
        );
        assertDoesNotThrow(
            () -> new PropertyDefinition(Property.MAINTAINABILITY,
                maintainabilityMetric,
                maintainabilityUnit,
                maintainabilityTarget,
                maintainabilityFailure)
        );
    }
}
