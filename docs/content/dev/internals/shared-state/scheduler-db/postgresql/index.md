# PostgreSQL Backend

The PostgreSQL backend implements `SchedulerDatabase` on a PostgreSQL relational database instead of the objectstore (Ceph RADOS or VFS). It is the backend CTA is moving to; the objectstore backend will be deprecated. For where it sits in the scheduler code, see the [code layout](../../index.md#code-layout); for how it differs from the objectstore, see the [backend comparison](../index.md#backend-comparison).

## Build and code

- **Build flag**: configure with `-D CTA_USE_PGSCHED=ON`, which defines `CTA_PGSCHED` (also referred to as *pgsched*). A build contains either the PostgreSQL or the objectstore backend, not both.
- **`scheduler/rdbms/`**: `RelationalDB` (the `SchedulerDatabase` implementation), mounts, jobs, requests and `TapeMountDecisionInfo`.
- **`scheduler/rdbms/postgres/`**: the row classes and SQL statements for each table and view.
- **`scheduler/rdbms/schema/postgres_scheduler_schema.sql`**: the schema, installed with `cta-scheduler-schema-create`.
- **`maintd/routines/scheduler/rdbms/`**: the cleanup routines run by `cta-maintd`.

## Status

The backend started as a copy of the `OStoreDB` interface in `scheduler/rdbms/` with every method throwing, implemented step by step until the frontends and `cta-taped` ran end to end. It now supports:

- archive, retrieve and repack workflows, including multi-copy files;
- garbage collection of jobs left behind by dead mounts.

Current work:

- scaling the deployment for performance tests;
- extending the scheduler algorithm unit tests, which were written for the objectstore, to both backends;
- testing the garbage collection routines.

`Scheduler` and `SchedulerDatabase` are only changed where the backend needs it; other improvements are tracked as tickets.

## Long-term target

Rethink parts of `Scheduler` and `SchedulerDatabase`, and refactor `scheduler/rdbms/`, to use what a relational database offers and remove scheduling biases introduced by the objectstore design. The [known limitations](mount-scheduling.md#known-limitations) list the current shortcomings this work should address.
