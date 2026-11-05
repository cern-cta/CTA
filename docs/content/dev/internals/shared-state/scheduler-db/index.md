!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Scheduler DB

The scheduler DB holds the queued work: archive, retrieve and repack jobs, which mount owns each of them, and what remains to be reported to the disk system. These pages describe the interface the scheduler logic uses to reach it, how its two backends implement that interface, and the backend-independent [scheduling algorithm](scheduling-algorithm.md) built on top. For what the scheduler does and why, see [Scheduling](../../../../concepts/data-management/scheduling.md).

The scheduler logic in `scheduler/` reaches the queues only through the abstract `SchedulerDatabase` class, implemented by `OStoreDB` (objectstore, `scheduler/OStoreDB/`) or `RelationalDB` (PostgreSQL, `scheduler/rdbms/`). See the [code layout](../index.md#code-layout) for how this fits with the catalogue and the low-level `objectstore/` and `rdbms/` layers.

!!! info "The "scheduler" is **not** a process"

    There is no "scheduler service" in CTA. The "scheduler" is a library (`scheduler/`) linked into the frontends, `cta-taped` and `cta-maintd`. **Scheduling is distributed**: every idle drive runs the [scheduling algorithm](scheduling-algorithm.md) for itself, reading and writing the scheduler DB and the catalogue.

## Backend contract

A backend implements `SchedulerDatabase` (`scheduler/SchedulerDatabase.hpp`). The `Scheduler` relies on it for:

| Responsibility | Interface |
| --- | --- |
| Queue work submitted by the frontends | `queueArchive`, `queueRetrieve`, `queueRepack` |
| Summarise unclaimed work into candidate mounts | `getMountInfoNoLock` (dry run), `getMountInfo` (takes the scheduling lock), returning a `TapeMountDecisionInfo` with one `PotentialMount` per queue summary |
| Create the chosen mount and release the lock | `TapeMountDecisionInfo::createArchiveMount`, `createRetrieveMount` |
| Hand out batches of jobs to a mount, owned by that mount | `ArchiveMount::getNextJobBatch`, `RetrieveMount::getNextJobBatch` |
| Record transfer outcomes and retries | `setJobBatchTransferred`, `ArchiveJob::failTransfer`, `RetrieveJob::failTransfer`, `requeueJobBatch` |
| Hand out jobs to report to the disk system, and record the reports | `getNextArchiveJobsToReportBatch`, `getNextRetrieveJobsToReportBatch`, `setArchiveJobBatchReported`, `setRetrieveJobBatchReportedToUser`, `failReport` |
| Inspection and operator actions | `getArchiveJobs`, `getRetrieveRequests…`, `cancelArchive`, `deleteFailed`, failed-job summaries |
| Repack expansion and reporting | `getNextRepackJobToExpand`, `getRepackReportBatches`, … |

The [Scheduling Algorithm](scheduling-algorithm.md#taking-the-first-candidate-tape-to-be-served) shows which of these calls each step of mount selection uses.

Both backends must guarantee that a job is owned by at most one mount at a time, that jobs of a mount that died are eventually released, and that a job is reported to the disk system once its outcome is final.

!!! note "TODO"
    State the concurrency guarantees each call must provide (which calls may run concurrently, what the scheduling lock must cover) and link the common and backend-specific tests.

## Backend comparison

Both backends run the same decision logic in `Scheduler.cpp`: the dry run, filtering, ranking and the walk over candidates are identical, and existing mounts are read from the catalogue's `DRIVE_STATE` table in both cases. What differs is what each backend implements behind `SchedulerDatabase`:

| Aspect | PostgreSQL | Objectstore |
| --- | --- | --- |
| Queues | Rows in the queue tables, summarised by views | One object per queue: `ArchiveQueueToTransferForUser` and `ArchiveQueueToTransferForRepack` (per tape pool), `RetrieveQueueToTransfer` (per VID), split into shards |
| Queue summaries | Aggregated by SQL over unclaimed jobs: one candidate per tape pool and mount policy (archive), or per VID, mount policy, activity and disk system (retrieve) | Kept up to date inside each queue object (`OStoreDB::fetchMountInfo`): one candidate per queue (archive), or per VID and activity, plus one for jobs without an activity (retrieve) |
| Scheduling lock | One per logical library | One `SchedulerGlobalLock` object for the whole instance, taken exclusively with a timeout. Every insert or pop also locks the queue object |
| Mount ID | Allocated in the scheduling transaction (`MountsRow::getNextMountID`) | Counter stored in the `SchedulerGlobalLock` object |
| Next-mount reservation | Written to the drive's `NEXT_*` columns when the mount is created | None: the mount is created and the lock released without writing a reservation |
| Retrieve tape choice | `selectBestVid4Retrieve`: active tapes first, then most work queued | `Helpers::selectBestRetrieveQueue`: cached tape status and queue statistics, more bytes queued or higher priority wins, disabled tapes only as a fallback; ties are broken by a choice that rotates daily |
| Order jobs are claimed | Priority, then archive file ID (archive) or job ID (retrieve) | Queue order, shard by shard, without sorting by priority. Retrieve shards are grouped by fSeq |
| Job ownership | Job rows carry the mount ID | Popped jobs are owned by the drive's agent object |
| Dead sessions | `cta-maintd` releases jobs of mounts whose heartbeat is old (`InactiveMountQueueRoutines`) | `cta-maintd` garbage-collects objects owned by agents whose heartbeat is old (`GarbageCollectRoutine`) |
| Report and failed jobs | Status of the job rows; jobs whose report cannot be delivered move to the failed tables | Separate to-report and failed queue objects, cleaned by `QueueCleanupRoutine` |
