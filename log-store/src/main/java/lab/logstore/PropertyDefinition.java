package lab.logstore;

public record PropertyDefinition(Property property, String metric,
    String unit, String target, String failure) {

    public static final String ERR_ILLEGAL_PROPERTY = "Property cannot be null.";
    public static final String ERR_ILLEGAL_METRIC = "Metric cannot be blank.";
    public static final String ERR_ILLEGAL_UNIT = "Unit cannot be blank.";
    public static final String ERR_ILLEGAL_TARGET = "Target cannot be blank.";
    public static final String ERR_ILLEGAL_FAILURE = "Failure cannot be blank.";

    public PropertyDefinition {
        if (property == null) {
            throw new IllegalArgumentException(ERR_ILLEGAL_PROPERTY);
        }

        if (metric == null || metric.isBlank()) {
            throw new IllegalArgumentException(ERR_ILLEGAL_METRIC);
        }

        if (unit == null || unit.isBlank()) {
            throw new IllegalArgumentException(ERR_ILLEGAL_UNIT);
        }

        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException(ERR_ILLEGAL_TARGET);
        }

        if (failure == null || failure.isBlank()) {
            throw new IllegalArgumentException(ERR_ILLEGAL_FAILURE);
        }
    }
}
