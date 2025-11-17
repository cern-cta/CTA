# cta-maintd

This directory contains the source code the maintenance daemon. The responsibility of the maintenance daemon is to periodically run a number of routines.

At the moment, this works as follows:

```mermaid
graph LR

ra1["RoutineA"]
rb1["RoutineB"]
rc1["RoutineC"]


ra2["RoutineA"]
rb2["RoutineB"]
rc2["RoutineC"]

subgraph threadA
direction LR
START:::hidden --> ra1 --> rb1 --> rc1
rc1 -- sleep --> ra2
ra2 --> rb2 --> rc2 --> END:::hidden
end

classDef hidden display: none;

```

The sleep interval can be configured in the config file.

## Routines

The routines are defined in `routines/`. Which routines are run depend on whether the Objectstore or Postgres scheduler is used

### Objectstore

- `DiskReportArchiveRoutine`
  - Reports the state (fail or success) of archive jobs to the disk instance.
- `DiskReportRetrieveRoutine`
  - Reports the state (fail or success) of retrieve jobs to the disk instance.
- `RepackExpandRoutine`
  - Expands repack requests into separate archive/retrieve jobs.
- `RepackExpandRoutine`
  - Takes care of the repack reporting.
- `GarbageCollectRoutine`
  - Performs garbage collection on agents and their objects in the objectstore.
- `QueueCleanupRoutine`
  - Finds queues marked for cleanup, takes ownership of these queues and moves the requests to other queues.

### Postgres

- `DiskReportArchiveRoutine`
  - Reports the state (fail or success) of archive jobs to the disk instance.
- `DiskReportRetrieveRoutine`
  - Reports the state (fail or success) of retrieve jobs to the disk instance.
- `RepackExpandRoutine`
  - Expands repack requests into separate archive/retrieve jobs.
- `RepackExpandRoutine`
  - Takes care of the repack reporting.
