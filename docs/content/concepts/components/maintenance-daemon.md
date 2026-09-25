# CTA Maintenance Daemon

The maintenance daemon (`cta-maintd`) advances background work through the scheduler without using a tape drive directly.

!!! note "A legacy name"
    “Maintenance” is a legacy name: the daemon also reports archive and retrieve results to the disk system as part of normal request processing. It is required for those workflows to progress, not just for occasional housekeeping.

## Reporting to the disk system

The daemon collects pending completion and failure reports from the scheduler and sends them to the [Disk Buffer](disk-buffer.md). The scheduler tracks reporting outcomes and handles retries when notification fails.

File transfer and reporting are separate steps. Tape daemons can finish transfers while their reports remain queued; if reporting is not running, the disk system may still be waiting for the outcome.

## Repack coordination

The daemon expands [Repack](../data-management/repack.md) requests into per-file work and processes the resulting retrieve and archive reports to advance the repack. Tape daemons perform the reads and writes; the maintenance daemon coordinates the background steps between them.

## Scheduler recovery and cleanup

Recovery and cleanup routines depend on the scheduler backend. They recover work left by failed agents or inactive mounts, handle abandoned reporting work, and remove stale coordination state.

For the objectstore backend, queue cleanup also supports tape-state transitions: queued retrievals may need to be reassigned to another tape copy or reported as failed before the transition finishes. See [Tape Lifecycle](../tape/lifecycle.md).

## Relationships with other components

The daemon uses the [Scheduler](scheduler.md) to claim and advance background work, the [Catalogue](catalogue.md) for metadata, and the disk system's reporting interface for notifications. Its routines run periodically, with the enabled set depending on the backend and deployment.

See [Maintenance Daemon Configuration](../../ops/configuration/maintenance-daemon.md) for routine selection and settings.
