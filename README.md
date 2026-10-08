# DS Lab

## Problem

This respository is a lab for durable key-value store. The first module is `log-store`. Today it only defines three properties. The append-only file is not built yet.

## Design

The parent build uses Java21. The `log-store` contains `Property` and `PropertyDefinition`. A `PropertyDefinition` with a blank metric, unit, target, or failure. The three values are in [Glossary](./docs/glossary.md).

## Failure modes

The tests reject a blank metric, a blank unit, a blanl target, and a blank failure. The module does not recover from a torn write, and it yet does not serve a get.

## How to run the tests

From the repository root, run `./mvnw -B test` command. The GitHub workflow [Daily Build](./.github/workflows/ci.yaml) uses the same command on every push.

## What would change at 10 times the load

There is no load test yet, so no measure number exists. At 1000 requests per second the get path would need a measured 99th percentile and the current in-memory definition would tell us nothing about that time.
