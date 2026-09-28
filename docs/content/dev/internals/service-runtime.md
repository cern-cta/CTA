!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Service Runtime

Shared service infrastructure lives in `lib/runtime/`. This page is for developers implementing or changing daemon lifecycle behaviour; deployed-service settings belong in [Operations](../../ops/deploy-and-configure/configuration/service-runtime.md).

## Application lifecycle

TODO: Document application construction, startup, shutdown, and ownership of shared services, with implementation entry points and a minimal daemon example.

## Configuration and command-line handling

TODO: Explain configuration loading, validation, common command-line options, and how a component adds its own settings.

## Signals and runtime state

TODO: Document signal handling, runtime-directory ownership, process metadata, and cleanup responsibilities.

## Health and telemetry integration

TODO: Explain when the runtime initializes telemetry and exposes health state. See [Readiness and Liveness](health-checks.md) and [Telemetry Internals](telemetry.md).

## Tests

TODO: Identify lifecycle and configuration tests and explain how to test failure, shutdown, and cleanup paths.
