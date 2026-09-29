# Metrics Conventions

Follow these conventions when adding or changing metrics. Log-message and log-attribute rules are covered separately in [Logging](logging.md#attributes-and-context).

## Names and units

- Reuse applicable [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/general/metrics/) before introducing CTA-specific names.
- Keep metric and attribute definitions centralized in the implementation, following the relevant language guide. The emitted names and meanings must remain consistent across languages; see [C++ definitions](cpp.md#define-constants) for the current implementation.
- Describe what is measured, including relevant success/failure semantics. Record values in the declared unit consistently across callers.
- Treat changes to names, units, instrument kinds, and attribute meanings as interface changes. Coordinate changes with consumers of dashboards and alerts rather than silently reusing a name for a different measurement.

## Attributes and cardinality

- Use metric attributes for bounded categories that are useful for aggregation, such as direction or outcome. Do not use file IDs, mount IDs, query text, or raw exception messages as metric attributes.
- Estimate the number of attribute combinations, not just the number of attributes. Each combination can create another time series, multiplied by the number of service instances and exported histogram buckets where applicable.
- Keep cardinality small and justified. There is no universal limit of ten; review the expected scale and cost before adding dimensions. See [Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/#do-not-overuse-labels).
- Use resource attributes for service and deployment identity. Do not repeat resource information on every recording unnecessarily.
- Avoid credentials or sensitive payloads in either metric or resource attributes.

## Recording behaviour

- Record at a clearly defined boundary and account for retries and partial failures. A metric must not change meaning depending on the caller.
- Reuse component instruments according to the language SDK’s lifecycle. Do not create instruments per request or per object instance.
- Keep recording inexpensive, particularly in hot paths. Measure overhead when instrumentation adds substantial work or runs at high frequency.
- Keep the application functional when telemetry is disabled. Adding a recording must not require callers to initialize exporters themselves.
