package lab.logstore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class PropertyDefinitionTest {
    private final String scalabilityMetric = "99th percentile time for a get, at 100 requests per second.";
    private final String scalabilityUnit = "milliseconds";
    private final String scalabilityTarget = "200 milliseconds";
    private final String scalabilityFailure = "The get request is slower than 200 milliseconds, so the caller waiting on it times out.";

    private final String reliabilityMetric = "Time to recover from crash that has tore last write.";
    private final String reliabilityUnit = "seconds";
    private final String reliabilityTarget = "60 seconds";
    private final String reliabilityFailure = "Recovery returns a half-written record, or it does not finish recovering.";

    private final String maintainabilityMetric = "Number of types touched to add a field to a stored record.";
    private final String maintainabilityUnit = "types";
    private final String maintainabilityTarget = "1 type";
    private final String maintainabilityFailure = "Adding a field forces edits in more than 1 type, so change is no longer local.";

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
        PropertyDefinition scalabilityDefinition = assertDoesNotThrow(
            () -> new PropertyDefinition(Property.SCALABILITY,
                scalabilityMetric, 
                scalabilityUnit,
                scalabilityTarget,
                scalabilityFailure)
        );

        assertEquals(scalabilityMetric, scalabilityDefinition.metric());
        assertEquals(scalabilityUnit, scalabilityDefinition.unit());
        assertEquals(scalabilityTarget, scalabilityDefinition.target());
        assertEquals(scalabilityFailure, scalabilityDefinition.failure());

        PropertyDefinition reliabilityDefinition = assertDoesNotThrow(
            () -> new PropertyDefinition(Property.RELIABILITY,
                reliabilityMetric,
                reliabilityUnit,
                reliabilityTarget,
                reliabilityFailure)
        );
        assertEquals(reliabilityMetric, reliabilityDefinition.metric());
        assertEquals(reliabilityUnit, reliabilityDefinition.unit());
        assertEquals(reliabilityTarget, reliabilityDefinition.target());
        assertEquals(reliabilityFailure, reliabilityDefinition.failure());

        PropertyDefinition maintainabilityDefinition = assertDoesNotThrow(
            () -> new PropertyDefinition(Property.MAINTAINABILITY,
                maintainabilityMetric,
                maintainabilityUnit,
                maintainabilityTarget,
                maintainabilityFailure)
        );
        assertEquals(maintainabilityMetric, maintainabilityDefinition.metric());
        assertEquals(maintainabilityUnit, maintainabilityDefinition.unit());
        assertEquals(maintainabilityTarget, maintainabilityDefinition.target());
        assertEquals(maintainabilityFailure, maintainabilityDefinition.failure());        
    }
}
