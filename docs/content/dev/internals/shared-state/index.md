# Shared State

The CTA processes (the frontends, `cta-taped` and `cta-maintd`) never call each other. They communicate through two shared stores, which every process reads and writes through the same libraries.

| Store | Holds | Lifetime | Backends |
| --- | --- | --- | --- |
| [Catalogue](catalogue/index.md) | Configuration (tapes, tape pools, VOs, storage classes, mount policies), the location of every file on tape, and the live state of every drive (`DRIVE_STATE`) | Persistent; the system of record | Oracle, PostgreSQL (SQLite in memory for unit tests) |
| [Scheduler DB](scheduler-db/index.md) | Queued archive, retrieve and repack jobs, which mount owns them, and what remains to be reported to the disk system | Until each job is reported, but must survive restarts | PostgreSQL, objectstore (Ceph RADOS or VFS) |

Neither store has a process of its own in front of it: the "scheduler" is a library too, and each idle drive runs the [scheduling algorithm](scheduler-db/scheduling-algorithm.md) for itself. For how a request moves through both stores, see the [request path](../workflows/index.md#the-request-path).

## Code layout

Each process links two libraries, one per store:

- **Catalogue** (`catalogue/`): the abstract `Catalogue` class, which groups one interface per kind of metadata (`Tape()`, `TapePool()`, `MountPolicy()`, `DriveState()`, `ArchiveFile()`, …, declared in `catalogue/interfaces/`). It is implemented on relational databases by the `Rdbms*Catalogue` classes in `catalogue/rdbms/`, with Oracle- and PostgreSQL-specific code under `catalogue/rdbms/oracle/` and `catalogue/rdbms/postgres/`. See the [catalogue design overview](catalogue/index.md#design-overview-of-the-cta-catalogue-and-the-supporting-rdbms-layer).
- **Scheduler** (`scheduler/`): the `Scheduler` class and the mount and job classes (`ArchiveMount`, `RetrieveMount`, `ArchiveJob`, `RetrieveJob`, …). It holds all backend-independent decisions, such as mount selection in `Scheduler::getNextMount`, and reaches the queued work through the abstract `SchedulerDatabase` class, implemented by `OStoreDB` (objectstore) or `RelationalDB` (PostgreSQL). See the [Scheduler DB overview](scheduler-db/index.md).

Below both, the **backend API** handles the low-level access: connections and statements for relational databases (`rdbms/`), used by the catalogue and by `RelationalDB`, and object handling for the objectstore (`objectstore/`).

```mermaid
flowchart TB
--8<-- "docs/includes/mermaid-classes.txt"

  PROC["<b>cta-workflow-api, cta-admin-api</b><br/><b>cta-taped, cta-maintd</b>"]

  subgraph LIB["Libraries"]
    direction LR
    subgraph CATLIB["catalogue/"]
      direction TB
      CAT["<b>Catalogue</b><br/>Tape(), MountPolicy(), DriveState(), …"]
      RCAT["<b>Rdbms*Catalogue</b><br/>catalogue/rdbms/"]
    end
    subgraph SCHLIB["scheduler/"]
      direction TB
      SCH["<b>Scheduler</b><br/>mounts and jobs"]
      SDB["<b>SchedulerDatabase</b><br/>abstract interface"]
      OST["<b>OStoreDB</b><br/>scheduler/OStoreDB/"]
      REL["<b>RelationalDB</b><br/>scheduler/rdbms/"]
    end
  end

  subgraph API["Backend API"]
    direction LR
    RDB["<b>rdbms</b><br/>rdbms/"]
    OBJ["<b>objectstore</b><br/>objectstore/"]
  end

  PROC --> CAT
  PROC --> SCH
  SCH --> CAT
  SCH --> SDB
  CAT -. "implemented by" .-> RCAT
  SDB -. "implemented by" .-> OST
  SDB -. "implemented by" .-> REL
  RCAT --> RDB
  REL --> RDB
  OST --> OBJ

  class PROC daemon
  class CAT,SCH,SDB api
  class RCAT,OST,REL store
  class RDB,OBJ ext
  class LIB,CATLIB,SCHLIB,API group
```

Solid arrows mean "uses"; dotted arrows point from an interface to its implementations.

## Who uses which store

Processes call the catalogue directly for metadata, and reach queued work through the `Scheduler` class. The `Scheduler` class and the scheduler DB backends also read the catalogue themselves, while queueing, choosing mounts and detecting dead mounts.

| Process | Catalogue, directly | Scheduler DB, via the `Scheduler` class |
| --- | --- | --- |
| `cta-admin-api` | Configuration commands: tapes, pools, VOs, storage classes, mount policies and rules, drives (`frontend/common/AdminCmd.cpp`) | Queue listings, repack requests, failed-job inspection |
| `cta-workflow-api` | Storage classes and archive files of incoming requests (`frontend/common/WorkflowEvent.cpp`) | Queueing archive and retrieve jobs, deletions and cancellations |
| `cta-taped` | Drive state, tape state and encryption keys, media types for RAO (`taped/session/`) | Choosing a mount, claiming and completing job batches, reporting drive status, recording files written to tape |
| `cta-maintd` | None of its own: it opens the catalogue and passes it to the `Scheduler` class and its routines | Reporting job results; its cleanup routines skip the `Scheduler` class and call `RelationalDB` directly (for example `RelationalDB::getDeadMounts`, which also reads live mount IDs from `DRIVE_STATE`) |

The catalogue's `DRIVE_STATE` table is where the two meet during scheduling: drives write their status there, and the scheduling algorithm reads it to count mounts per VO and to see which tapes are in use. Its current-mount and next-mount columns are described in [Drive state and next mounts](scheduler-db/postgresql/mount-scheduling.md#drive-state-and-next-mounts).
