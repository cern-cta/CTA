!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Workflows

These pages trace requests through CTA code and persistent state. For user-visible behaviour, start with [Concepts](../../../concepts/index.md); disk-system protocols belong in [Disk System Integrations](../../guides/integrations/index.md).

| Operation | Implementation guide |
| --- | --- |
| Archive data to tape | [Archive](archival.md) |
| Retrieve data from tape | [Retrieve](retrieval.md) |
| Remove file metadata and pending work | [Delete](deletion.md) |
| Move or recreate tape copies | [Repack](repack.md) |

Deleted and repacked copies are kept in the catalogue's [Recycle Bin](../shared-state/catalogue/recycle-bin.md).

## The request path

!!! abstract "In short"

    - Every archive or retrieve request becomes one or more **jobs** in the scheduler DB.
    - No central component hands out work. Each idle **drive asks for its own mount** and, once mounted, **claims batches of jobs** from that one queue until it is empty.
    - `cta-maintd` **reports** the outcome of each job back to the disk system.

Archive and retrieve requests follow the same path: the workflow frontend checks them against the catalogue and queues jobs in the scheduler DB, a tape daemon picks the jobs up and transfers the data, and the maintenance daemon reports the result. The processes never call each other; they communicate through the [shared state](../shared-state/index.md): the **scheduler DB** holds the queues of jobs, the **catalogue** holds the long-lived configuration (tapes, pools, VOs, mount policies) and the live state of every drive.

```mermaid
flowchart LR
--8<-- "docs/includes/mermaid-classes.txt"

  subgraph EXT["External"]
    direction TB
    DS["<b>Disk system</b><br/>EOS, dCache"]
    OP["<b>Operators</b><br/>cta-admin"]
  end

  subgraph FE["Frontend"]
    direction TB
    WF["<b>cta-workflow-api</b>"]
    ADM["<b>cta-admin-api</b>"]
  end

  subgraph STATE["Shared state"]
    direction TB
    CAT[("<b>Catalogue</b><br/>tapes, pools, VOs,<br/>policies, drive state")]
    SDB[("<b>Scheduler DB</b><br/>job queues")]
  end

  subgraph TAPE["Tape servers"]
    direction TB
    TD["<b>cta-taped</b><br/>one per drive"]
    MD["<b>cta-maintd</b>"]
  end

  DS  -->|"archive / retrieve<br/>request"| WF
  OP  -->|"admin commands,<br/>repack"| ADM

  WF  -->|"look up copies,<br/>policy"| CAT
  WF  -->|"insert jobs"| SDB
  ADM -->|"configuration,<br/>drive up/down"| CAT
  ADM -->|"queue listings,<br/>repack"| SDB

  TD  -->|"choose mount,<br/>claim jobs"| SDB
  TD  -->|"report<br/>drive state"| CAT
  MD  -->|"report results,<br/>clean up"| SDB

  TD  <==>|"file data"| DS
  MD  -.->|"success / failure"| DS

  class DS,OP ext
  class WF,ADM api
  class CAT,SDB store
  class TD,MD daemon
  class EXT,FE,STATE,TAPE group
```

| Component | Role in the request path |
| --- | --- |
| `cta-workflow-api` | Receives workflow events from the disk system (archive, retrieve, delete, cancel). Resolves the file, its storage class and its mount policy in the catalogue and inserts jobs into the scheduler DB (`frontend/common/WorkflowEvent.cpp`). See [Workflow Frontend](../components/workflow-frontend.md). |
| `cta-admin-api` | Serves `cta-admin`: configuration, drive up/down, queue listings and queuing repack requests (`frontend/common/AdminCmd.cpp`). See [Admin Frontend](../components/admin-frontend.md). |
| `cta-taped` | One process per drive. When the drive is idle, it decides which mount to do, runs the session and claims jobs. All scheduling decisions happen here. |
| `cta-maintd` | Reports finished jobs back to the disk system and cleans up after mounts that died. See [Maintenance Daemon](../components/maintenance-daemon.md). |
| Scheduler DB | Job queues and their summaries, behind the `SchedulerDatabase` interface. See [Scheduler DB](../shared-state/scheduler-db/index.md). |
| Catalogue | Configuration, plus the `DRIVE_STATE` table recording what each drive is doing. See [Catalogue](../shared-state/catalogue/index.md). |

The path has three steps, described below: [queuing](#1-queuing-requests-become-jobs), [scheduling and transfer](#2-scheduling-and-transfer), and [job outcomes and reporting](#job-outcomes-and-reporting).

## 1. Queuing: requests become jobs

```mermaid
flowchart LR
--8<-- "docs/includes/mermaid-classes.txt"

  DS["<b>Disk system</b>"]
  OP["<b>Operators</b>"]
  WF["<b>cta-workflow-api</b>"]
  ADM["<b>cta-admin-api</b>"]
  CHK{"Catalogue<br/>lookups"}
  REF["Refused"]
  AJ["<b>Archive</b><br/>one job per tape copy,<br/>in its tape pool queue"]
  RJ["<b>Retrieve</b><br/>pick one VID,<br/>one job in its queue"]
  RP["<b>Repack</b><br/>jobs in separate<br/>repack queues"]
  SDB[("<b>Scheduler DB</b><br/>unclaimed jobs")]

  DS  -->|"request"| WF --> CHK
  CHK -->|"failed"| REF
  CHK -->|"archive"| AJ
  CHK -->|"retrieve"| RJ
  OP  -->|"repack"| ADM --> RP
  AJ & RJ & RP --> SDB

  class DS,OP,REF ext
  class WF,ADM api
  class SDB store
```

### Resolving scheduling parameters

When the frontend receives a request it looks up, in the catalogue, every parameter the scheduler will need later. It answers two questions:

- **Where does the data go, or come from?** For archive, the storage class and its archive routes give the number of copies and the tape pool of each copy. For retrieve, the archive file ID resolves to the tape copies of the file.
- **How urgent is it?** A [mount rule](../../../concepts/data-management/storage-model.md#mount-policies-and-requesters) selects the request's mount policy, which sets its priority and minimum request age. Rule resolution is in `catalogue/rdbms/RdbmsMountPolicyCatalogue.cpp`, called through `getArchiveFileQueueCriteria` (archive) and the retrieve equivalent.

If any lookup fails (no archive route, no matching mount rule, unknown archive file ID…) the request is refused. Each accepted request is acknowledged synchronously to the disk system.

The policy's name, priority and minimum request age are copied into each job. When choosing a mount the scheduler overrides the copied values with the current ones from the catalogue ([Input 2](../shared-state/scheduler-db/scheduling-algorithm.md#input-2-current-mount-policies)); the copied priority is still what orders jobs within a batch claim in the PostgreSQL backend.

### How jobs are added

Entry points are `SchedulerDatabase::queueArchive` and `SchedulerDatabase::queueRetrieve`.

**Archive:** One job is queued **per tape copy**, each for the copy's tape pool. No tape is chosen yet: any writable tape in the pool will do. It will be up to one of the suitable tape servers to decide which.

**Retrieve:** The file may exist on several tapes. The scheduler picks **one tape** up front and queues **one job** for that VID; the other copies are kept as alternates in case the chosen one fails. How the tape is picked is backend-specific (see [Retrieve tape choice](../shared-state/scheduler-db/index.md#backend-comparison)).

**Repack** queues retrieves for the source tape and archives for the destination pools, in separate repack queues so repack never competes with user jobs for queue rows.

A new job is unclaimed. Nothing happens to it until a drive comes looking for work. With the PostgreSQL backend, concurrent queueing requests can be merged into one insert; see [Opportunistic Batching](../shared-state/scheduler-db/postgresql/opportunistic-batching.md).

## 2. Scheduling and transfer

Each idle drive's `cta-taped` reads queue summaries, the state of all drives and the VO limits, ranks the candidate mounts and creates the best one it can serve. Once the tape is mounted, it claims batches of jobs from that one queue and transfers them until the queue is empty. Nothing preempts a running mount.

How the drive chooses, how candidates are filtered and ranked, and how batches are claimed is described in [Scheduling Algorithm](../shared-state/scheduler-db/scheduling-algorithm.md).

## Job outcomes and reporting

Once a mount has claimed a job, the transfer result decides what happens next:

| Outcome | What happens | Who handles it next |
| --- | --- | --- |
| Transfer OK | Marked to report success. With several archive copies, the first copies wait until the last one is on tape. | `cta-maintd` reports to the disk system and deletes the job |
| Failed, retries left in this mount | Requeued, still owned by this mount, so no other drive sees it | This mount, on its next batch |
| Failed, this mount's retries used up | Requeued unowned | Any drive; it shows up in the summaries again |
| Failed, all retries used up | Marked to report failure | `cta-maintd` reports the error |
| Report could not be delivered after all report retries | Moved to the failed queue | Operators |
| Drive or daemon died mid-session | Jobs stay owned by the dead session | `cta-maintd` releases them once the session's heartbeat is old |

How ownership, failed queues and dead-session detection are stored differs per backend; see the [backend comparison](../shared-state/scheduler-db/index.md#backend-comparison). For the PostgreSQL tables each outcome moves a job between, see [Job lifecycle across the tables](../shared-state/scheduler-db/postgresql/schema.md#job-lifecycle-across-the-tables); for dead-session cleanup, see [PostgreSQL Garbage Collection](../shared-state/scheduler-db/postgresql/garbage-collection.md).

TODO: Each workflow page should explain its failure, retry, cancellation, and completion-reporting behaviour alongside its normal path.
