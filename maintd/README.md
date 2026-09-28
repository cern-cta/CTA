# Maintenance daemon

`cta-maintd` runs periodic scheduler work, including disk reporting, repack, and backend-specific cleanup.

- `RoutineRunner.cpp`: routine execution loop.
- `RoutineRunnerFactory.cpp`: backend-specific routine selection.
- `routines/`: individual routine implementations.
- `MaintdConfig.hpp`: configuration definitions and validation.

See [Maintenance Daemon internals](../docs/content/dev/internals/components/maintenance-daemon.md) for execution and routine behavior, and [Operations configuration](../docs/content/ops/deploy-and-configure/configuration/maintenance-daemon.md) for deployment settings.
