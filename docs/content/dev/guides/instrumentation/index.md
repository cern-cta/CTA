# Instrumentation

Use this section to add useful logs and metrics to CTA code. Logs describe individual events; metrics aggregate measurements to show behaviour over time. The conventions describe emitted signals independently of language. Framework-specific APIs and lifecycle details are documented in separate implementation guides.

- [Logging](logging.md): follow message, severity, and attribute conventions.
- [Metrics](metrics.md): understand instruments and add a metric to CTA.
- [Metrics Conventions](semantic-conventions.md): choose names, units, and bounded attributes.
- [Testing Metrics](testing.md): check measurements and inspect locally exported metrics.

## Language guides

- [C++ Instrumentation](cpp.md): logging APIs, metric registration, and framework-specific test helpers.

Add Rust or Python guides here as their component instrumentation is established. They should describe API usage and map to the shared conventions above, rather than duplicate them. No project-specific Rust or Python instrumentation workflow is documented yet.

Provider initialization and process identity are covered in [Telemetry Internals](../../internals/telemetry.md). Service-health implementation belongs in [Readiness and Liveness](../../internals/health-checks.md). Production collection, dashboards, and alerting belong in [Operations](../../../ops/run-and-maintain/monitoring/health-and-alerts.md).
