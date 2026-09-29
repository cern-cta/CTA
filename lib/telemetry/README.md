# Telemetry library

C++ OpenTelemetry initialization and shared metric instruments.

- `include/telemetry/`: public initialization and instrumentation interfaces.
- `src/metrics/`: instruments and registry implementation.
- `src/TelemetryInit.cpp` and `src/OtelInit.cpp`: initialization paths.

To add instrumentation, use the [C++ instrumentation guide](../../docs/content/dev/guides/instrumentation/cpp.md) and [metrics conventions](../../docs/content/dev/guides/instrumentation/metrics.md). For provider initialization, instrument registration, and lifecycle changes, see [Telemetry Internals](../../docs/content/dev/internals/telemetry.md).
