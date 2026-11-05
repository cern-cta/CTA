# Scheduler DB

The **Scheduler DB** is the persistent backend that holds CTA's in-flight work: archive, retrieve and repack requests, the job "queues" built from them, and the coordination state needed while that work is pending, in progress or being reported. It is separate from the [Catalogue](catalogue.md), which holds long-lived metadata and configuration, even when both use PostgreSQL.

## Role in scheduling

CTA has no scheduler service or process. All scheduling logic is contained in a library linked into all services that queue, inspect and execute work. The scheduler DB, together with the catalogue, is the shared state through which those services coordinate. No central component hands out work: each idle tape daemon reads queue summaries from the scheduler DB, chooses the best eligible mount it can serve, and then claims that mount's jobs from it.

See [Scheduling](../data-management/scheduling.md) for batching, priorities and the distinction between eligibility and immediate service, and the [Scheduling Algorithm](../../dev/internals/shared-state/scheduler-db/scheduling-algorithm.md) for how a drive chooses its mount.

## What it stores

- **Requests and jobs**: each archive or retrieve request becomes one or more jobs, queued per tape pool (archive) or per tape (retrieve).
- **Job state**: which session owns a job, retry counts, and the results waiting to be reported back to the disk system.
- **Repack state**: the progress of repack requests and the jobs they generate.
- **Coordination state**: what services need to avoid conflicting decisions, such as scheduling locks or, with the PostgreSQL backend, mount reservations.

Request state is shorter-lived than archive metadata, but it is not disposable. It persists across service restarts, and losing it means losing the work it tracks, even if the catalogue and recorded tape copies remain intact.

## Use by services

| Service | Uses the scheduler DB to… |
| --- | --- |
| Workflow Frontend | Queue file requests as jobs. |
| Admin Frontend | Inspect and manage queued work, and start repacks. |
| Tape daemon | Read queue summaries to choose its next mount, claim jobs and update their outcomes. |
| Maintenance daemon | Send queued reports, advance repack work and recover or clean up stale state. |

## Backends

CTA has objectstore and PostgreSQL scheduler DB backends. The PostgreSQL backend is intended to replace the objectstore backend in the future. The mount decision logic is shared by both; how queues are stored, summarised, locked and claimed depends on the backend.

Backend implementations do not necessarily expose identical inspection and administrative capabilities. Consult the documentation for the deployed release and the [PostgreSQL Backend](../../dev/internals/shared-state/scheduler-db/postgresql/index.md) when assessing that backend.

Independent scheduler DBs (even on different backends) can share a catalogue, for example to separate repack from client workloads. Each retains its own work and service connections; see [Catalogue and scheduler topology](index.md#catalogue-and-scheduler-topology). Operational guidance belongs in [Scheduler Configuration](../../ops/deploy-and-configure/configuration/scheduler.md) and [Scheduling and Queues](../../ops/run-and-maintain/administration/requests.md).
