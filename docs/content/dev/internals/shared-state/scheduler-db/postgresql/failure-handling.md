!!! info "PostgreSQL scheduler"

    This page describes the PostgreSQL scheduler backend.

# Failure handling

How a failed archive or retrieve job reaches a terminal state in the PostgreSQL scheduler backend:
which copies fail together, where the row ends up, and how the failure is accounted for. Two axes
run through all of it — user versus repack, and a *transfer* failure versus a *report* failure — and
they do not behave the same way. [Who fails together](#who-fails-together-the-userrepack-split) is
the short version of that matrix; the lifecycle diagrams further down trace every path in full.

The mechanism underneath all of them is batching, covered first.

## Batching failed-job reports

Reporting a failure back to the scheduler DB is on the same hot path as reporting a success, but it
used to pay for it file by file: each failed read, write, or report cost its own scheduler-DB round
trip, serializing DB connection usage exactly when a drive, library, or disk endpoint is already in
trouble (many files failing in quick succession). Batching merges many jobs' failure reports into
one SQL statement, the same idea as [opportunistic batching](opportunistic-batching.md) applied to
the report path instead of the queueing path.

Two related but distinct mechanisms are built this way, triggered from different places: batching
*transfer*-retry decisions below, and
[batching *report*-retry decisions](#batching-failed-report-retries) further down.

Both are implemented for the postgres scheduler backend (pgsched) only. The objectstore backend
always reports file by file; its `prepareFailTransfer()`/`failTransferBatch()` (and the report-side
equivalents) are unimplemented stubs (`NotImplementedException`) for exactly this reason — see
[Why the single-job path still exists](#why-the-single-job-path-still-exists) below.

```mermaid
flowchart TB
    T1(("task fails")) -->|prepareFailTransfer| A["accumulator\n(m_failedArchiveJobs /\nm_failedRetrieveJobs)"]
    T2(("task fails")) -->|prepareFailTransfer| A
    TN(("task fails")) -->|prepareFailTransfer| A

    A -->|"count >= 600,\nor a flush event"| F["flushFailedJobs()"]
    F --> M["ArchiveMount::failTransferBatch() /\nRetrieveMount::failTransferBatch()\n(bucket by isRepack x exceeded-retries)"]
    M --> R["failTransferBatchRequeue()\n(one VALUES-CTE UPDATE)"]
    M --> E["failTransferBatchExceedRetries()\n(one VALUES-CTE UPDATE)"]

    classDef action fill:#3498db,color:#ffffff,stroke:#333,stroke-width:1px;
    class A,F,M,R,E action;
```

### In-memory accumulation

When a task fails, `MigrationReportPacker`/`RecallReportPacker` call `prepareFailTransfer(reason)` on
the job. This updates the job's in-memory state only — it appends to `failureLogs`/`reportFailureLogs`
and bumps the retry counters — and does **not** touch the DB. The job is then pushed onto an
in-process accumulator (`m_failedArchiveJobs` / `m_failedRetrieveJobs`).

The accumulator is flushed (`flushFailedJobs()`) whenever any of these happen, whichever comes first:

- it reaches `c_failedJobsBatchMax` (600, hardcoded, identical for archive and retrieve);
- a `ReportFlush` event fires (the same periodic/count-driven event — governed by `flushMaxFiles`/
  `flushMaxBytes` — that also flushes the successful-jobs batch to EOS, see
  [below](#two-different-batch-sizes-on-purpose));
- `ReportLastBatchError` or `ReportEndofSession` runs.

So in practice 600 only matters as a *worst case within one flush window*: a dense burst of failures
(e.g. a dying drive failing every file) can hit it before the next periodic flush would have fired
anyway. If `failTransferBatch()` throws, the whole in-flight batch is dropped (not retried) and left
for the garbage collector to pick up later — see [Garbage Collection](garbage-collection.md).

### The batched SQL

`flushFailedJobs()` hands the accumulated jobs to `ArchiveMount::failTransferBatch()` /
`RetrieveMount::failTransferBatch()`, which buckets them by `isRepack` and by whether each job has
exceeded `maxTotalRetries`, then issues **one** SQL statement per bucket:

- `ArchiveJobQueueRow`/`RetrieveJobQueueRow`::`failTransferBatchRequeue()` — under budget, moves the
  whole bucket back to `(REPACK_)*_PENDING_QUEUE` in one `VALUES`-CTE `UPDATE`.
- `failTransferBatchExceedRetries()` — budget exceeded, sets the whole bucket's status to
  `*_ToReportTo{User,Repack}ForFailure` in one statement.

This is live for repack on both archive and retrieve, as well as for ordinary user jobs — the
counters and retry/exceed logic are identical; only the destination queue/status differs.

#### Append-once `FAILURE_LOG`

Each row's `FAILURE_LOG`/`REPORT_FAILURE_LOG` column must never be duplicated across retries.
`updateJobRowFailureLog()` appends onto a value that was *already loaded from the DB column* at
row-construction time, so by the time the batched SQL runs, the in-memory string already contains
old+new text. The SQL binds that string directly (`:FL`) rather than re-concatenating it with
`||`/`COALESCE` against the DB column — the few places that still use `||` are bound from a fresh
literal marker (e.g. `"UNPROCESSED_TASK_QUEUE_JOB_REQUEUED"`), not from the loaded log, and are
correctly additive.

#### Repack reporting is a separate mechanism entirely

Repack's failure *reporting* to the operator (aggregate `FILE_COUNT`/`FILE_BYTES` stats per repack
request) is architecturally unrelated to anything on this page: `getNextFailedArchiveRepackReportBatch()` /
`getNextFailedRetrieveRepackReportBatch()` work off aggregate counters via
`moveFailedRepackJobBatchToFailedQueueTable()` + `RepackRequestTrackingRow::updateRepackRequestFailuresBatch()`.
There is no per-job `FAILURE_LOG` concatenation and no retry-count concept there; it never reaches
disk/EOS reporting at all. What this section describes is purely the *transfer*-retry mechanism
(deciding whether a failed job gets another attempt or is given up on) — repack jobs go through it
exactly like user jobs, but repack's own *reporting* of the outcome to the operator is a different
layer downstream. Repack does, however, reach the *other* batching mechanism this page covers —
see [Batching failed report retries](#batching-failed-report-retries) below.

### Batching failed report retries

A second, distinct batching mechanism covers a different kind of failure: not the tape transfer
itself, but the two things that happen *after* a transfer succeeds — persisting that success to
the scheduler DB, and (for user jobs only) reporting it back to EOS. Both reuse the same
in-memory-accumulation idea (`prepareFailReport()`, the report-side mirror of
`prepareFailTransfer()` above) and the same per-bucket SQL shape
(`failReportBatchRequeue()`/`failReportBatchExceedRetries()`), but are triggered from two
different places:

```mermaid
flowchart TB
    S1(("ArchiveMount::reportJobsBatchTransferred()\nthrows FailedReportMoveToQueue")) --> FB["ArchiveMount::failReportBatch()\n(bucket by isRepack x exceeded report-retries)"]
    S2(("Scheduler::report(Archive/Retrieve)JobsBatch()\nEOS/disk report call fails")) -->|"prepareFailReport\n(user jobs only)"| FB2["RelationalDB::fail(Archive/Retrieve)JobBatchReport()"]
    FB --> RR["failReportBatchRequeue()"]
    FB --> EE["failReportBatchExceedRetries()"]
    FB2 --> RR
    FB2 --> EE

    classDef action fill:#3498db,color:#ffffff,stroke:#333,stroke-width:1px;
    class FB,FB2,RR,EE action;
```

1. **Mount-level bookkeeping failure** (`ArchiveMount::failReportBatch()`, called from
   `MigrationReportPacker::reportSuccessfulJobsBatch()`'s `FailedReportMoveToQueue` catch): the
   tape write and catalogue update both succeeded, but the live mount's own
   `setJobBatchTransferred()`/`setRepackJobBatchTransferred()` DB write failed. This is the one
   place repack jobs reach report-failure batching — see below. Retrieve has no equivalent: it
   never writes to the catalogue, so there is no comparable two-phase write to recover from.
2. **Disk-report failure** (`RelationalDB::failArchiveJobBatchReport()` /
   `failRetrieveJobBatchReport()`, called from `Scheduler::reportArchiveJobsBatch()` /
   `reportRetrieveJobsBatch()`): the job's success was already persisted, but the later,
   independent EOS/disk report of it then failed. Repack never reaches this path —
   `getNextArchiveJobsToReportBatch()` only ever selects user jobs, since repack never reports to
   disk/EOS at all (see [above](#repack-reporting-is-a-separate-mechanism-entirely)).

Both funnel into the same `ArchiveJobQueueRow`/`RetrieveJobQueueRow`::`failReportBatchRequeue()`/
`failReportBatchExceedRetries()` pair, bucketed the same way as the transfer-failure path above:
under budget requeues (`IS_REPORTING = FALSE`, retry counters bumped, status otherwise unchanged);
over budget — or `NoReportRequired`, which is permanently true for every repack job — is terminal.

#### Repack's "terminal" failure here isn't a failure

For repack, landing in the "exceeded report retries" bucket doesn't mean the *job* failed — it
means the DB *write* of an already-successful transfer failed repeatedly. Recovery means replaying
that write via `updateRepackJobSuccess()`, exactly like the single-job path
(`ArchiveRdbJob::failReport()` → `updateJobStatusForFailedReport()`) already does, not moving the
row to `ARCHIVE_FAILED_QUEUE`: only `updateRepackJobSuccess()` knows whether a multi-copy request's
other copies have also succeeded, and bypassing it would risk
`getNextSuccessfulArchiveRepackReportBatch()` deleting the source file before every copy exists.
`ArchiveJobQueueRow::failReportBatchExceedRetries()` branches on `isRepack` for exactly this reason
— the non-repack branch's UPDATE+INSERT+DELETE sequence (a genuine terminal failure, moving the row
to `ARCHIVE_FAILED_QUEUE`) does not apply to repack at all.

## Who fails together: the user/repack split

Both mechanisms above decide one thing before they touch the DB: when a job fails terminally, does
it drag the other copies of its request down with it? The answer differs between the user and the
repack workflow, and the difference is deliberate — getting it wrong in either direction causes data
to be mishandled, so it is worth stating once, plainly, for all eight combinations.

| Workflow | Transfer failure, retries exhausted | Report failure, retries exhausted |
|---|---|---|
| **archive · user · single copy** | `AJS_ToReportToUserForFailure`, this `JOB_ID` | `ReadyForDeletion`, row moved to `ARCHIVE_FAILED_QUEUE` |
| **archive · user · multi copy** | **every** copy of the `ARCHIVE_REQUEST_ID`, together | **every** copy of the `ARCHIVE_REQUEST_ID`, moved together |
| **archive · repack · single copy** | `AJS_ToReportToRepackForFailure`, this `JOB_ID` | not a failure at all — `updateRepackJobSuccess()` recovers the lost write |
| **archive · repack · multi copy** | this `JOB_ID` **only**; siblings keep transferring | same recovery, still per `JOB_ID` |
| **retrieve · user** | `RJS_ToReportToUserForFailure`, this `JOB_ID` | `ReadyForDeletion`, row moved to `RETRIEVE_FAILED_QUEUE` |
| **retrieve · repack** | `RJS_ToReportToRepackForFailure`, this `JOB_ID` | n/a — repack retrieve never reports to a disk system |

Retrieve has no multi-copy rows to reconcile: a retrieve request is one row per file, and its other
tape copies live in that same row's `ALTERNATE_VIDS`/`ALTERNATE_FSEQS` columns, used to rotate to
another tape on requeue rather than to fan out into sibling jobs. So every retrieve row, user or
repack, is keyed on `JOB_ID` alone on both the transfer and the report side.

### Why user requests fail as a group

For a user archive request the copies exist to satisfy a redundancy goal. Once one copy is
permanently lost the request cannot meet that goal, so there is nothing to gain by letting the
others run: `failTransferBatchExceedRetries()` and `failReportBatchExceedRetries()` both expand to
every copy of the `ARCHIVE_REQUEST_ID` when `REQUEST_JOB_COUNT > 1`, matching the single-job
`updateFailedJobStatus()` and `moveJobToFailedQueueTable()`.

It is safe to settle a sibling copy that a drive is still writing, because **nothing in the user
workflow reads "all copies are terminal" as permission to delete anything**. The source file belongs
to the user's disk system; CTA only reports the failure to it. The one thing that has to be guarded
is the reverse direction — that still-writing sibling eventually succeeding and resurrecting the
request — and `updateMultiCopyJobSuccess()` does that with a single predicate:
`STATUS <> AJS_ToReportToUserForFailure`.

### Why repack copies must fail alone

Repack is the case that paragraph does not apply to, because repack **does** own a shared source
file: the repack buffer copy on disk, which every copy of the file reads from and which CTA itself
deletes. The deletion is gated on the whole group having settled —
`getNextFailedArchiveRepackReportBatch()` calls `deleteDiskFiles()` once
`moveFailedRepackJobBatchToFailedQueueTable()` reports the group complete, and
`getNextSuccessfulArchiveRepackReportBatch()` does the same on the success side once the group
reaches `ReadyForDeletion` — and that completeness is inferred from `STATUS` alone.

Marking a copy another drive is still writing as failed makes that inference false. The group would
read as complete, the buffer would be deleted from under a live reader, and a copy that nevertheless
finished writing would land in the catalogue with no scheduler row left to report it. So a failed
repack copy settles alone and the group waits; it is swept up, surviving successes included, only
once every copy has reached a terminal status of its own.

That wait costs nothing in the statistics. The repack counters are per **copy**, not per file:
`Scheduler::expandRepackRequest()` increments `totalFilesToArchive` once per copy it adds to
`copyNbsToRearchive`, and both the success path (`deleteSuccessfulRepackArchiveJobBatch()`) and the
failure path (`getNextFailedArchiveRepackReportBatch()`) credit one per row. A request is complete
when `archivedFiles + failedArchiveFiles + failedToCreateArchiveReq >= totalFilesToArchive`, and it
ends `RRS_Failed` if *any* failure was recorded (`repackRequestStatusSql()`). The safety property
operators rely on — a repack that lost a copy does not end `RRS_Complete`, so its source tape is not
reclaimed — therefore comes from `failedArchiveFiles > 0`, not from how many copies were counted as
failed.

!!! note "Known attribution gap"

    When a group is swept after one copy failed, every row in it is credited as a failure, including
    a sibling copy that genuinely reached tape. Totals still balance and the request still ends
    `RRS_Failed`, so nothing unsafe follows, but `cta-admin repack ls` under-reports archived files
    and over-reports failures by the number of surviving copies. Fixing it means returning `STATUS`
    from `moveFailedRepackJobBatchToFailedQueueTable()` and bucketing the swept rows into the
    archived and failed counters separately; the group gate itself would not change, since the
    buffer deletion needs it either way.

## Invariants the group logic rests on

Four properties hold the repack grouping together. Each has been a bug at least once, and each is
load-bearing for the shared buffer file's lifetime.

**`REQUEST_JOB_COUNT` equals the number of archive rows the file actually has.**
`transformJobBatchToArchive()` inserts exactly one archive job per entry in
`REPACK_REARCHIVE_COPY_NBS` — the same contract the objectstore scheduler keeps by construction, one
`SorterArchiveJob` per `copyNbsToRearchive` entry — so
`rows == CARDINALITY(REPACK_REARCHIVE_COPY_NBS) == REQUEST_JOB_COUNT`, and that is also the request's
`TOTAL_FILES_TO_ARCHIVE` contribution. `BASE_INSERT` is therefore gated on the retrieved copy being
one of the copies to re-archive: true for `MoveOnly` and `MoveAndAddCopies`, where
`expandRepackRequest()` puts the moved copy's own `copyNb` in the set, and false for
`AddCopiesOnly`, where the tape is read but not drained and only the missing copies are queued. A
count below the real group size makes every completeness check pass on the first copy to settle.

**A group is `(REPACK_REQUEST_ID, ARCHIVE_FILE_ID)`, never `ARCHIVE_FILE_ID` alone.** The same file
can be in two repack requests at once — two tapes each holding one of its copies, both being
repacked. Those rows are not siblings: each request has its own buffer copy, its own completeness
and its own statistics. Counting them together both stalls genuinely complete groups and lets one
request's progress sweep away the other's rows.

**A row is modified once per statement.** PostgreSQL discards a `DELETE` of a row that the same
statement has already updated — all sub-statements of one statement share a snapshot and a command
id, so `ExecDelete` sees `TM_SelfModified` and skips the row without an error. Every queue-to-queue
move is therefore shaped `DELETE … RETURNING *` first, `INSERT` second, and the user report-failure
move (which has to write a status as well) is split into two statements rather than chained into one
CTE. Folding it back together strands the row in the active queue at `ReadyForDeletion`, where
nothing collects it.

**Row locks never block across groups.** `moveFailedRepackJobBatchToFailedQueueTable()` takes both
its candidate lock and its sibling lock with `SKIP LOCKED`. A blocking sibling lock deadlocks two
concurrent reporters working the two copies of one file — each holds the row the other wants, and
PostgreSQL aborts one with `40P01`, losing a whole batch. Skipping cannot deadlock and fails in the
safe direction: a sibling locked elsewhere drops out of the count, the group reads as incomplete,
and the move is deferred to whichever pass finds the group uncontended.

## The full archive lifecycle: every path to a terminal state

Putting both mechanisms above together, end to end, for archive (where every nuance lives — retrieve
is the simpler subset covered [separately](#the-full-retrieve-lifecycle) below). Four terminal
dispositions exist, and which one a job reaches depends on *where* it stops succeeding, not just
*whether* it eventually does:

| Terminal state | Meaning | Who reaches it |
|---|---|---|
| **Back to `ARCHIVE_PENDING_QUEUE`** | Retry, counters preserved | Transfer failure, under budget |
| **Stays in `ARCHIVE_ACTIVE_QUEUE`**, `STATUS` changed | Interim — something still needs to happen | Every non-terminal step below |
| **`ARCHIVE_FAILED_QUEUE`** (row moved, deleted from active) | CTA could not fulfil an obligation — admin-visible | Transfer retries exhausted and reported; OR a report/bookkeeping write that itself couldn't be delivered |
| **Deleted outright, no `ARCHIVE_FAILED_QUEUE` row** | CTA's obligation was fulfilled, even if the *outcome* was a failure | A failure that WAS successfully reported to EOS; a repack request where every copy reached `ReadyForDeletion` |

```mermaid
flowchart TB
    START(("job dispatched to\na mount for tape write")) --> XFER{"tape write\nsucceeds?"}

    XFER -- no --> PFT["prepareFailTransfer()\n(accumulate in m_failedArchiveJobs)"]
    PFT --> FTB["ArchiveMount::failTransferBatch()\nbucket: isRepack x maxTotalRetries exceeded"]
    FTB -- under budget --> FTR["failTransferBatchRequeue()"]
    FTR --> PENDING["(REPACK_)ARCHIVE_PENDING_QUEUE\nAJS_ToTransferFor(User/Repack)\nretry counters preserved"]
    PENDING -. refetched by a future mount .-> XFER
    FTB -- "exceeded\nUSER: whole ARCHIVE_REQUEST_ID\ngroup fails together\nREPACK: this copy only" --> FTE["failTransferBatchExceedRetries()"]
    FTE --> TOREPORTFAIL["ACTIVE_QUEUE\nAJS_ToReportTo(User/Repack)ForFailure"]

    XFER -- yes --> RJBT["ArchiveMount::reportJobsBatchTransferred()"]
    RJBT --> CAT{"catalogue\nupdate OK?"}
    CAT -- "no: FailedReportCatalogueUpdate" --> FTB
    CAT -- yes --> DBW{"setJobBatchTransferred() /\nsetRepackJobBatchTransferred()\nDB write OK?"}

    DBW -- "no: FailedReportMoveToQueue" --> FRB["ArchiveMount::failReportBatch()\nbucket: isRepack x\n(NoReportRequired OR retries exceeded)"]
    FRB -- "requeue\n(see note 1 below)" --> FRR1["failReportBatchRequeue()"]
    FRR1 --> RETRYWRITE["ACTIVE_QUEUE, IS_REPORTING=false\nstatus unchanged - retry the write"]
    RETRYWRITE -. next flush .-> DBW
    FRB -- "exceeded - USER\n(in practice: always, see note 1)" --> FRE1U["failReportBatchExceedRetries(false)"]
    FRE1U --> FAILEDQ1["ARCHIVE_FAILED_QUEUE\nfile IS on tape + catalogued -\nonly the DB bookkeeping is missing"]
    FRB -- "exceeded - REPACK\n(always: NoReportRequired)" --> URS2["updateRepackJobSuccess()\n(recover the write - not a failure)"]

    DBW -- yes --> SPLIT{isRepack?}
    SPLIT -- no --> TOREPORTOK["ACTIVE_QUEUE\nAJS_ToReportToUserForSuccess"]
    SPLIT -- yes --> URS1["updateRepackJobSuccess()"]

    URS1 --> MC{"multi-copy: every sibling\n(same REPACK_REQUEST_ID +\nARCHIVE_FILE_ID) also\nAJS_ToReportToRepackForSuccess?"}
    URS2 --> MC
    MC -- "no" --> WAITING["ACTIVE_QUEUE (this copy only)\nAJS_ToReportToRepackForSuccess"]
    WAITING -. last sibling completes .-> MC
    MC -- "yes, or single-copy" --> RFD["ACTIVE_QUEUE (ALL copies together)\nReadyForDeletion"]

    TOREPORTFAIL --> GETBATCH["getNextArchiveJobsToReportBatch()\n(USER only - repack never reports to EOS)"]
    TOREPORTOK --> GETBATCH
    GETBATCH --> EOSCALL["Scheduler::reportArchiveJobsBatch()\nasync EOS/disk report"]
    EOSCALL --> EOSOK{"EOS report\nsucceeds?"}
    EOSOK -- yes --> SETREPORTED["setArchiveJobBatchReported()"]
    SETREPORTED -- "was ForSuccess" --> DELOK["DELETED\nEOS + catalogue both have it"]
    SETREPORTED -- "was ForFailure" --> DELFAIL["DELETED, no ARCHIVE_FAILED_QUEUE row\nEOS now knows it failed -\nCTA's obligation is fulfilled"]
    EOSOK -- no --> PFR["prepareFailReport()\n(accumulate)"]
    PFR --> FRB2["RelationalDB::failArchiveJobBatchReport()\n(no isRepack param - never sees repack,\nreportType is genuinely fresh here)"]
    FRB2 -- under budget --> FRR2["failReportBatchRequeue(false)"]
    FRR2 --> RETRYEOS["ACTIVE_QUEUE, IS_REPORTING=false\nstatus unchanged (still ForSuccess\nor ForFailure) - retry"]
    RETRYEOS -. next report pass .-> GETBATCH
    FRB2 -- exceeded --> FRE2["failReportBatchExceedRetries(false)"]
    FRE2 --> FAILEDQ2["ARCHIVE_FAILED_QUEUE\neven if the transfer succeeded -\nEOS could never be told"]

    RFD --> REPACKOK["maintd RepackReportRoutine:\ngetNextSuccessfulArchiveRepackReportBatch()"]
    REPACKOK --> REPACKDEL["DELETED, repack request credited\n(archivedFiles/Bytes), SOURCE disk\nfile deleted - all copies done first"]
    TOREPORTFAIL -. "repack copy\n(see note 2 below)" .-> REPACKFAIL["maintd routine:\ngetNextFailedArchiveRepackReportBatch()"]
    REPACKFAIL --> GRP{"every copy of the group\nsettled (success or failure)?"}
    GRP -- no --> WAITFAIL["ACTIVE_QUEUE (this copy only)\nAJS_ToReportToRepackForFailure\nwaits for its siblings"]
    WAITFAIL -. last sibling settles .-> GRP
    GRP -- "yes, or single-copy" --> REPACKFAILQ["REPACK_ARCHIVE_FAILED_QUEUE\n(whole group, successes included),\nrequest's failedFiles credited,\nSOURCE file deleted - safe now"]

    classDef pending fill:#f39c12,color:#fff,stroke:#333;
    classDef active fill:#3498db,color:#fff,stroke:#333;
    classDef okterm fill:#27ae60,color:#fff,stroke:#333;
    classDef failterm fill:#c0392b,color:#fff,stroke:#333;

    class PENDING pending;
    class TOREPORTFAIL,TOREPORTOK,WAITING,WAITFAIL,RFD,RETRYWRITE,RETRYEOS active;
    class DELOK,DELFAIL,REPACKDEL okterm;
    class FAILEDQ1,FAILEDQ2,REPACKFAILQ failterm;
```

**Note 1 — the mount-level bookkeeping path (`ArchiveMount::failReportBatch()`) never actually
retries, for either job type.** The job object reaching it still carries the `reportType` it was
given at `initialize()` time, when `getNextJobBatch()` fetched it — at that point its `STATUS` was
`AJS_ToTransferFor(User/Repack)`, which `ArchiveRdbJob::initialize()` maps to `NoReportRequired`
(see the `switch` there). Nothing re-evaluates `reportType` between then and
`failReportBatch()` — a successful transfer changes what the row's `STATUS` *should* become, but
that happens only in the DB write that just failed, not on the in-memory object. So
`reportType == NoReportRequired` is true for every job reaching this function, user or repack,
and the bucket condition (`NoReportRequired || retries exceeded`) is therefore always true: the
`requeue` bucket for THIS entry point is unreachable in practice. For repack that is harmless —
`failReportBatchExceedRetries()`'s repack branch is just `updateRepackJobSuccess()`, safe to call
on every attempt. **For user jobs it means a single transient failure of
`setJobBatchTransferred()` sends the job straight to `ARCHIVE_FAILED_QUEUE`**, never actually
spending any of `maxReportRetries` at this layer — worth a second look independently of the
batching work this page otherwise covers, since it looks unintentional rather than designed. The
*other* report-failure entry point, `RelationalDB::failArchiveJobBatchReport()`, does not have this
problem: it is fed by a fresh `getNextArchiveJobsToReportBatch()` fetch, whose `initialize()` call
sees the real `AJS_ToReportToUserFor{Success,Failure}` status, so `reportType` is genuinely
`CompletionReport`/`FailureReport` there and the retry budget is honoured.

**Note 2 — repack's two sides gate the source-file deletion symmetrically.**
`updateRepackJobSuccess()` only reaches `ReadyForDeletion` — and so only lets
`getNextSuccessfulArchiveRepackReportBatch()` delete the buffer file — once every copy of the
`(REPACK_REQUEST_ID, ARCHIVE_FILE_ID)` group has succeeded.
`moveFailedRepackJobBatchToFailedQueueTable()` mirrors that from the other direction: it moves a
failed copy's whole group, surviving successes included, only once every copy
(`REQUEST_JOB_COUNT` of them) has itself settled, success or failure. A copy still mid-transfer is
never counted and never touched, so the caller's `deleteDiskFiles()` cannot run while a sibling
still needs to read the buffer. The two checks are deliberately blind to each other's statuses: a
transfer failure is invisible to the success-side count (so a waiting successful copy neither
stalls nor promotes on its own), and it is the failure-side sweep that eventually settles such a
mixed group. See [Who fails together](#who-fails-together-the-userrepack-split) for why repack needs
this gate and the user workflow does not.

## The full retrieve lifecycle

Retrieve is the same shape with two whole branches removed, because retrieve never writes to the
catalogue and has no mount-level bookkeeping-write-recovery concept at all — `RetrieveMount` has no
`failReportBatch()`, and no `setRepackJobBatchTransferred()`-equivalent two-phase write to fail in
the first place:

```mermaid
flowchart TB
    START(("job dispatched to\na mount for tape read")) --> XFER{"tape read\nsucceeds?"}

    XFER -- no --> PFT["prepareFailTransfer()\n(accumulate)"]
    PFT --> FTB["RetrieveMount::failTransferBatch()\nbucket: isRepack x maxTotalRetries exceeded"]
    FTB -- under budget --> FTR["failTransferBatchRequeue()"]
    FTR --> PENDING["(REPACK_)RETRIEVE_PENDING_QUEUE\nretry counters preserved"]
    PENDING -. refetched .-> XFER
    FTB -- exceeded --> FTE["failTransferBatchExceedRetries()"]
    FTE --> TOREPORTFAIL["ACTIVE_QUEUE\nRJS_ToReportTo(User/Repack)ForFailure"]

    XFER -- yes --> SJBT["RetrieveMount::setJobBatchTransferred()\n(no catalogue write, no two-phase\nbookkeeping failure possible)"]
    SJBT --> SPLIT{isRepack?}
    SPLIT -- no --> TOREPORTOK["ACTIVE_QUEUE\nRJS_ToReportToUserForSuccess\n(or deleted directly if no\nreport URL was requested)"]
    SPLIT -- yes --> REPACKOK["ACTIVE_QUEUE\nRJS_ToReportToRepackForSuccess"]

    TOREPORTFAIL --> GETBATCH["getNextRetrieveJobsToReportBatch()\n(USER only)"]
    TOREPORTOK --> GETBATCH
    GETBATCH --> EOSCALL["Scheduler::reportRetrieveJobsBatch()"]
    EOSCALL --> EOSOK{"report\nsucceeds?"}
    EOSOK -- yes --> SETREPORTED["setRetrieveJobBatchReportedToUser()"]
    SETREPORTED --> DEL["DELETED either way -\nsame fulfilled-obligation logic as archive"]
    EOSOK -- no --> PFR["prepareFailReport()"]
    PFR --> FRB["RelationalDB::failRetrieveJobBatchReport()\n(never sees repack - see below)"]
    FRB -- under budget --> FRR["failReportBatchRequeue()"]
    FRR --> RETRY["ACTIVE_QUEUE, IS_REPORTING=false - retry"]
    RETRY -. next pass .-> GETBATCH
    FRB -- exceeded --> FRE["failReportBatchExceedRetries()"]
    FRE --> FAILEDQ["RETRIEVE_FAILED_QUEUE"]

    REPACKOK --> TRANSFORM["RetrieveJobQueueRow::transformJobBatchToArchive()\n(maintd routine)"]
    TRANSFORM --> NEWARCHIVE["new AJS_ToTransferForRepack row(s)\nin ARCHIVE_PENDING_QUEUE - one per\ndestination copy - retrieve row gone"]
    TOREPORTFAIL -. repack copy .-> REPACKFAIL2["RelationalDB::getNextFailedRetrieveRepackReportBatch()\n(no group gate needed: a retrieve\nrow is one row per file)"]
    REPACKFAIL2 --> REPACKFAILQ2["REPACK_RETRIEVE_FAILED_QUEUE,\nrequest's failedFiles credited"]

    classDef pending fill:#f39c12,color:#fff,stroke:#333;
    classDef active fill:#3498db,color:#fff,stroke:#333;
    classDef okterm fill:#27ae60,color:#fff,stroke:#333;
    classDef failterm fill:#c0392b,color:#fff,stroke:#333;

    class PENDING pending;
    class TOREPORTFAIL,TOREPORTOK,REPACKOK,RETRY active;
    class DEL,NEWARCHIVE okterm;
    class FAILEDQ,REPACKFAILQ2 failterm;
```

Retrieve's repack success isn't a terminal state at all — `transformJobBatchToArchive()` turns a
successfully-retrieved repack job directly into one or more new `ARCHIVE_PENDING_QUEUE` rows (one
per destination tape pool, for `MoveAndAddCopies`), which then go through the entire archive
lifecycle above from the top. `failRetrieveJobBatchReport()` never sees repack, same reasoning as
the [archive-reporting section above](#repack-reporting-is-a-separate-mechanism-entirely): repack
retrieve failures are tracked by `getNextFailedRetrieveRepackReportBatch()`'s own aggregate counters
instead, going through `RetrieveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable()`.

## Two different batch sizes, on purpose

`c_failedJobsBatchMax` (600) is easy to confuse with `flushMaxFiles` (the config key governing how
many *successful* jobs accumulate before `reportSuccessfulJobsBatch()` reports them to EOS — e.g.
8000 in the stress-scale CI preset), but they bound different things and are tuned independently:

| | Failed-job batch (`c_failedJobsBatchMax`) | Successful-job batch (`flushMaxFiles`) |
|---|---|---|
| Downstream call | Scheduler-DB `UPDATE` only | One XRootD/EOS WFE call + DB write |
| Configurable? | No — compile-time `constexpr`, same for every deployment | Yes — per-mount-type Helm/config value |
| Blast radius if the batch call throws | Whole batch dropped, rescued later by GC | Whole batch falls back to a slow per-file retry path (documented in `ArchiveMount.hpp`) |

The asymmetry is a real gap worth knowing about rather than an active bug: `flushMaxFiles` has been
raised for scale testing (2000 → 4000/8000) while `c_failedJobsBatchMax` never moved and isn't even
wired to config. In practice this rarely bites, because failed jobs are *also* flushed on every
periodic `ReportFlush` event regardless of count (see above) — so 600 only matters during an unusually
dense failure burst within a single flush window. But if a future deployment needs to push
`flushMaxFiles` much higher for scale, revisit `c_failedJobsBatchMax` at the same time: a failure
storm is exactly the moment you don't want the system falling back to smaller, more numerous DB
round trips than the success path was tuned for.

## Why the single-job path still exists

`ArchiveRdbJob`/`RetrieveRdbJob`'s `failTransfer()` → `requeueFailedJob()`/`updateFailedJobStatus()`/
`handleExceedTotalRetries()` is the pre-batching, one-job-at-a-time mechanism. Under `CTA_PGSCHED`, no
production taped session calls it any more — `MigrationReportPacker`/`RecallReportPacker` only ever
call it from their `#else` (objectstore) branch.

It cannot simply be deleted or made to throw, because it is part of the common
`SchedulerDatabase::ArchiveJob`/`RetrieveJob` interface that both backends must implement, and
`SchedulerDatabaseTest.cpp` exercises it directly (`popAndRequeueArchiveRequests`) against *both*
backends via `INSTANTIATE_TEST_CASE_P`. That generic test cannot be switched to the batched
`prepareFailTransfer()`/`failTransferBatch()` calls instead, because those two methods are
deliberately unimplemented (`throw NotImplementedException`) on the objectstore side — batching is
pgsched-only by design. So `failTransfer()` stays as the one call both backends genuinely support,
even though the taped session's own report packers never reach it under pgsched today.
