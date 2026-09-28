!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Scheduler Schema Upgrades

This page reserves the PostgreSQL scheduler migration procedure, separate from [catalogue schema upgrades](catalogue-schema/index.md). It does not establish a supported in-place migration path.

## Compatibility and preparation

TODO: Document which CTA and scheduler schema versions work together, how to identify the installed schema, and the supported transition for the target release. Include backup and handling of pending work.

## Migration procedure

TODO: Document service coordination, the supported migration mechanism, and verification. Schema creation and deletion commands are not an upgrade procedure.

## Failure recovery and validation

TODO: Document recovery options and checks for queued work, scheduling, reporting, and service health before resuming normal traffic.
