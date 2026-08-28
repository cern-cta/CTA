# Tape Daemon Internals

The [tape daemon concepts](../../../concepts/components/tape-daemon.md) describe the lifetimes, state transitions, and transfer pipelines.
This page explains the implementation boundaries and invariants to preserve when changing `cta-taped`.
Paths below are relative to the repository root.

## Code map

| Area | Entry points and responsibility |
| --- | --- |
| Process integration | `taped/cta-taped.cpp` enters the common runtime; `TapedApp` constructs the catalogue, scheduler context, and daemon and exposes stop and health callbacks. |
| Daemon lifetime | `taped/TapeDaemon` manages registration and successive drive sessions; `DriveStartupPolicy` separates startup policy from execution. |
| Hardware ownership | `taped/session/DriveSession` prepares the drive, selects scheduler mounts, and decides whether another tape session may run. |
| Cartridge and transfer lifetime | `taped/session/TapeSession` assembles the archive or retrieve pipeline and coordinates completion and reporting. |
| Hardware boundary | `taped/drive/` provides drive operations, emptiness probing, and cleanup; cartridge movements go through `MediaChangerFacade`. |
| State and diagnostics | `catalogue/TapeDrivesCatalogueState` separates desired-state requests from reported state; `TapeSessionTracker` and `TapeSessionReporter` collect and publish session progress and outcomes. |

`TapeDaemon::registerDrive()` stores the scheduler backend name in drive configuration before marking registration complete.

`SchedulerContext` encapsulates scheduler backend setup.
Transfer code works through scheduler mount and job interfaces; backend-specific lifecycle work belongs outside the transfer pipeline.

## Ownership and cleanup

Tape-session cleanup establishes reuse after a normal transfer.
Do not add physical cleanup to drive-session destruction or daemon shutdown: hardware ownership may already have ended.
If a tape session throws without returning an outcome, recovery runs under the existing drive ownership before another mount is allowed, independently of `drive.clean_on_up`.

`DriveCleaner` can borrow an already-open drive.
It reports cleanup results and progress; the enclosing drive session owns the final `Down` publication.
An empty drive alone is insufficient for reuse if resetting persistent drive configuration failed.

`DriveReservation` currently tracks ownership within the process only.
It does not exclude another process from the device and is not a SCSI reservation.

## Worker lifetime and completion

Pipeline workers borrow session resources, including buffers, reporting objects, and hardware interfaces.
All workers must stop before those resources are destroyed or recovery cleanup accesses the drive.

`WorkerFailureHandling.hpp` deliberately exits without stack unwinding when an exception escapes worker startup or joining and termination cannot be established.
Ordinary exception recovery here could destroy borrowed state or operate the drive while a worker still uses it.
This path requires recovery on restart and cannot guarantee final reporting or cartridge cleanup.

The [transfer pipeline description](../../../concepts/components/tape-daemon.md#transfer-threads-and-buffers) explains archive flush boundaries and retrieval completion after tape cleanup.

## Outcomes and state publication

`DriveSession` reports `Preparing` before acquiring hardware, `Up` after successful preparation, and `Down` after releasing ownership.
Cleanup reports its tape-session phases while the drive remains `Preparing`; an emptiness probe has no tape session.

`TapeDrivesCatalogueState` maps drive and session reports to the existing catalogue status field without changing desired state.
`Preparing` maps to `Starting`; active session phases take precedence, while Loading, Finalizing, and Finished leave the previous status unchanged.
Legacy clients can therefore display Up during preparation; this does not establish readiness for scheduled work.

`TapeSessionResult` separates session success from drive reusability.
A failed session can leave the drive reusable; successful transfers can still be followed by cleanup or reporting failures.
Do not use either property as a substitute for the other.

Failure counters identify failed operations, not root causes; informational events and TapeAlerts alone do not mark a session unsuccessful.
The [diagnostics reference](../../../ops/run-and-maintain/monitoring/tape-session-diagnostics.md) documents the published fields.

Finishing a drive session must not overwrite a newer desired `Up` simply because it reports `Down`.
Failure and shutdown paths explicitly request desired `Down` and preserve an existing specific reason where possible.
That preservation currently uses a non-atomic read and update, so a concurrent desired-state change can be overwritten.
