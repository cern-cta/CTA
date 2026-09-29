# Telemetry Internals

This page is for developers changing telemetry initialization and lifecycle. To add instrumentation, start with [Metrics](../guides/instrumentation/metrics.md).

## Implementation entry points

- `lib/telemetry/include/telemetry/` exposes initialization and component instruments.
- `lib/telemetry/src/metrics/` contains instrument definitions and the registry implementation.
- `lib/telemetry/src/TelemetryInit.cpp` handles the existing programmatic configuration; `OtelInit.cpp` provides the newer initialization path.
- `common/semconv/` holds the shared metric and attribute definitions.

Keep initialization changes consistent with the service runtime and the supported configuration path. Do not initialize a new provider in ordinary application code merely to add a metric.

## Instrument lifecycle

`InstrumentRegistrar` invokes an initialization function when registered and retains it for later reinitialization. Component instruments initially use the default provider, allowing applications that do not configure telemetry to use the no-op path. Provider initialization recreates the instruments against the configured provider.

Central definitions make instruments discoverable and keep their lifecycle under one owner. Callers should use the shared variables rather than retain pointers that become stale after reinitialization. Registration and replacement of these variables are lifecycle operations; thread-safe recording does not make concurrent pointer replacement safe.

OpenTelemetry distinguishes identical instrument definitions from conflicting definitions. Repeated creation is not universally a semantic error; CTA's shared-registration pattern is its implementation convention. See the [OpenTelemetry Metrics API](https://opentelemetry.io/docs/specs/otel/metrics/api/).

## Process identity and temporality

Resource identity must distinguish simultaneous metric writers. The existing configuration can generate a service-instance UUID or derive a retained identity from hostname and process name. Retained identities can reduce series churn for repeatedly created processes, but must not cause concurrently active writers to share an identity. Check restart and fork behaviour when changing this logic.

Cumulative metrics represent measurements since their start or reset; delta metrics represent an interval. Keep exporter and backend expectations compatible, and verify counter resets and histogram behaviour when changing configuration. The standard Prometheus path uses cumulative series. Collector configuration and production backend choices belong in Operations.

## SDK dependency

CTA uses `opentelemetry-cpp`, with dependency packaging maintained in the [cta-dependencies repository](https://gitlab.cern.ch/cta/cta-dependencies/). Consult `project.json` for the selected dependency constraints. Library upgrades require validation of initialization, exporter configuration, and emitted metrics; they are separate from adding an application instrument.
