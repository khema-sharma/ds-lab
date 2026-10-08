# Glossary

## Scalability

Metric: 99th percentile time for a get, at 100 requests per second.

Unit: milliseconds

Target: 200 milliseconds

Failure this exposes: The get request is slower than 200 milliseconds, so the caller waiting on it times out.

## Reliability

Metric: Time to recover from crash that has tore last write.

Unit: seconds

Target: 60 seconds

Failure this exposes: Recovery returns a half-written record, or it does not finish recovering.

## Maintainability

Metric: Number of types touched to add a field to a stored record.

Unit: types

Target: 1 type

Failure this exposes: Adding a field forces edits in more than 1 type, so change is no longer local.

## Versions recorded on day 1

Java: 21.0.12

Docker: 29.8.1

Docker Compose: v5.5.1