# CTA Maintenance Daemon

The maintenance daemon runs background work that does not require a tape drive.

## Responsibilities

- Report archive and retrieve results to the disk system.
- Expand repack requests into retrieve and archive jobs, and process their results.
- Recover work left behind by failed scheduler agents where supported by the backend.
- Clean up affected queues during tape-state transitions.

## Queue cleanup

A tape-state transition can remain pending while queued requests are reassigned to another tape copy or reported as failed. Once cleanup finishes, the requested tape state takes effect. See [Tape Lifecycle](../tape/lifecycle.md).

## Configuration

The required routines depend on the scheduler backend. Operators should use [Maintenance Daemon Configuration](../../ops/configuration/maintenance-daemon.md); [Repack](../data-management/repack.md) explains the shared workflow.
