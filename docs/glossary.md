# Glossary

## Scalability

Metric: 99th percentile for a get, at 100 reads per second, against the target.
Unit: milliseconds

Target: 200

Failure this exposes: The get request slower than the target and the caller will time out.

## Reliability

Metric: Write failures, at 100 writes per second, against the target.

Unit: count

Target: 0

Failure it exposes: The write request, at rate higher than 100 writes per second, and the caller will get error.

## Maintainability

Metric: Cyclometric complexity of the code base against the target.

Unit: count

Target: 10

Failure it exposes: New team member making changes will see production issues.

## Versions recorded on day 1

Java: 21.0.12

Docker: 29.8.1

Docker Compose: v5.5.1