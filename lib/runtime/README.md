# Runtime library

Shared C++ application infrastructure for configuration, command-line parsing, logging, signals, health, and telemetry.

See [Service Runtime](../../docs/content/dev/internals/service-runtime.md) for the application model, examples, lifecycle, and tests. For packaging and deployment, see [RPM SPEC and Service Integration](../../docs/content/dev/guides/conventions/rpm-packaging.md) and [runtime configuration](../../docs/content/ops/deploy-and-configure/configuration/service-runtime.md).

- `include/runtime/`: public application and configuration interfaces.
- `src/`: shared implementations.
- `test/`: lifecycle, configuration, argument-parsing, signal, and health tests.
- `cta-logging.schema.json`: the versioned log schema; follow the [logging compatibility rules](../../docs/content/dev/guides/instrumentation/logging.md) when changing it.
