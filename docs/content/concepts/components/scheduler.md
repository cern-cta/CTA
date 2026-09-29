# Scheduler

The **scheduler** is the logic CTA services use to queue work and allocate tape mounts. Its **backend** persists requests, queues and coordination state.

## Responsibilities

The scheduler combines queued archive, retrieve and repack work with catalogue policies and resource availability to select eligible mounts. Tape daemons then perform the transfers.

See [Scheduling](../data-management/scheduling.md) for batching, priorities and the distinction between eligibility and immediate service.

## Relationships with services

| Service | Uses the scheduler to… |
| --- | --- |
| Workflow Frontend | Submit file requests. |
| Admin Frontend | Inspect and manage work. |
| Tape daemon | Claim mounts and jobs, and update their outcomes. |
| Maintenance daemon | Process reports, advance repack work and clean up backend state. |

## Backends

CTA has objectstore and PostgreSQL scheduler backends. The PostgreSQL scheduler is intended to replace the objectstore backend in the future. Both persist queued work across service restarts; this state is separate from the [Catalogue](catalogue.md), even when both use PostgreSQL.

Backend implementations do not necessarily expose identical inspection and administrative capabilities. Consult the documentation for the deployed release and the [PostgreSQL scheduler overview](../../dev/internals/components/scheduler/postgresql.md) when assessing that backend; do not infer feature parity from the shared scheduler interface.

Independent backends can share a catalogue, for example to separate repack from client workloads. Each retains its own work and service connections; see [Catalogue and scheduler topology](index.md#catalogue-and-scheduler-topology). Operational guidance belongs in [Scheduler Configuration](../../ops/deploy-and-configure/configuration/scheduler.md) and [Scheduling and Queues](../../ops/run-and-maintain/administration/requests.md).
