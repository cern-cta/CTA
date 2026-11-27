# PostgreSQL Repack Workflows

This page documents how the three repack types (`MoveOnly`, `AddCopiesOnly`, `MoveAndAddCopies`) drive the PostgreSQL backend's retrieve and archive queues differently, and the two invariants that make that work: which copy numbers a retrieve subrequest treats as "to rearchive", and how retrieve-progress counting stays correct regardless of which copies that turns out to be. The tables and columns referred to here are described in [PostgreSQL Schema](schema.md).

## The three repack types

`Scheduler::expandRepackRequest()` (`scheduler/Scheduler.cpp`) walks every file on the tape being repacked and decides, per file, which copy numbers the resulting retrieve subrequest must write to disk and rearchive - recorded as `RepackSubRequest::copyNbsToRearchive`:

| Type | What copy numbers go into `copyNbsToRearchive` | Why |
| --- | --- | --- |
| `MoveOnly` | The copy being read off the repacked tape itself. | The file is moving to a new tape; the copy on the old tape is being replaced. |
| `AddCopiesOnly` | Only copy numbers the storage class now requires that the file does **not** already have anywhere - **never** the copy being read off the repacked tape. | The existing copies, including the one on the repacked tape, are untouched; only the missing ones are created. |
| `MoveAndAddCopies` | Both: the copy being read off the repacked tape (as `MoveOnly`), plus any other missing copies (as `AddCopiesOnly`). | The file is moving *and* gaining copies in the same pass. |

`AddCopiesOnly` is the one case where the copy a subrequest **retrieves from** is guaranteed to never be a copy it **archives to**. Every mechanism below exists because of that asymmetry.

## Active copy vs. job: two different things a retrieve subrequest tracks

`RetrieveRequest` (`scheduler/rdbms/RetrieveRequest.cpp`) tracks two related but distinct pieces of per-subrequest state:

- **The active copy number** (`m_actCopyNb`, set by `setActiveCopyNumber()`): which existing tape copy this subrequest physically reads from. There is always exactly one, chosen by `RepackRequest::addSubrequestsAndUpdateStats()` from the file's existing `tapeFiles` (`--no-recall` takes the repacked VID directly; otherwise `Helpers::selectBestVid4Retrieve` picks it).
- **Jobs** (`m_jobs`, filled by `fillJobsSetRetrieveFileQueueCriteria()`/`initialize()`): one entry per archive copy this subrequest will create once the retrieve succeeds - built by scanning the file's existing tape copies and keeping only the ones whose copy number is in `copyNbsToRearchive`.

For `MoveOnly` and `MoveAndAddCopies`, the active copy's number is always in `copyNbsToRearchive` (see the table above), so a job always exists for it. For `AddCopiesOnly` it never is: the active copy is read, not rearchived, and `copyNbsToRearchive` only ever holds genuinely missing copy numbers. `setActiveCopyNumber()` must not assume a job exists for the active copy - it derives `fSeq`/`blockId`/`vid` from the active tape file directly instead (which must exist, since the active copy was chosen from the file's own tape copies), and only relies on a job when one exists, to set `m_status` from it. The caller always sets the retrieve status explicitly afterward anyway (`RepackRequest.cpp`'s `setJobStatus()` call, right after `setActiveCopyNumber()`).

The destination tape pool for each copy in `copyNbsToRearchive` comes from `RepackInfo::archiveRouteMap`, built from every configured archive route for the file's storage class - not filtered to `copyNbsToRearchive` - so it also has an entry for the active copy's own route even when that copy isn't being rearchived. `RetrieveRequest::insert()` only reads `archiveRouteMap` for entries in `copyNbsToRearchive`, so this is harmless; nothing routes the active copy through it when it isn't a target.

## Transforming a successful retrieve into archive jobs

Once a repack retrieve settles (`RJS_ToReportToRepackForSuccess`), `RetrieveJobQueueRow::transformJobBatchToArchive()` (`scheduler/rdbms/postgres/RetrieveJobQueue.cpp`) turns each row into one archive job per entry in its `REPACK_REARCHIVE_COPY_NBS` column (the DB form of `copyNbsToRearchive`), via two INSERTs into `REPACK_ARCHIVE_PENDING_QUEUE`:

- **`BASE_INSERT`**: writes the row's own `COPY_NB`/`TAPE_POOL` (the active copy) - but only when `EXISTS (... WHERE copy IN REPACK_REARCHIVE_COPY_NBS AND copy = COPY_NB)`. For `MoveOnly`/`MoveAndAddCopies` this is always true (the active copy is in the set). For `AddCopiesOnly` it is always false, by construction - `BASE_INSERT` writes nothing.
- **`ALTERNATE_INSERT`**: writes one row per remaining entry in `REPACK_REARCHIVE_COPY_NBS` (excluding the active copy, if present), using `REPACK_REARCHIVE_TAPE_POOLS` for each one's destination.

So for `AddCopiesOnly`, every archive job for a file comes from `ALTERNATE_INSERT`; for `MoveOnly`, every archive job comes from `BASE_INSERT`; `MoveAndAddCopies` uses both.

## Retrieve-progress counting must not depend on which insert ran

`REPACK_REQUEST_TRACKING.RETRIEVED_FILES`/`RETRIEVED_BYTES` have to count every file genuinely read off tape this batch, regardless of which of the two inserts above (if any) its recalled copy went on to feed. `transformJobBatchToArchive()` computes this with its own `RETRIEVED_FILE_STATS` CTE, aggregated straight from the batch's moved rows:

```sql
RETRIEVED_FILE_STATS AS (
  SELECT
    REPACK_REQUEST_ID,
    COUNT(*) AS RETRIEVED_FILES,
    COALESCE(SUM(SIZE_IN_BYTES), 0) AS RETRIEVED_BYTES
  FROM MOVED_ROWS
    WHERE NOT (MOUNT_ID IS NULL AND STATUS = :NORECALL_STATUS)
  GROUP BY REPACK_REQUEST_ID
)
```

`MOUNT_ID IS NULL AND STATUS = RJS_ToReportToRepackForSuccess` identifies a user-provided file (the repack "tape repair" workflow: a file injected straight into the repack buffer, never actually mounted and read) - the one case that must not count as retrieved. Every other moved row counts, independent of `BASE_INSERT`/`ALTERNATE_INSERT`. This used to be derived from `BASE_INSERTED_COUNT` minus a separately-computed not-retrieved count instead, which gave the right answer only when `BASE_INSERT` fired for every genuinely-retrieved row - true for `MoveOnly`/`MoveAndAddCopies`, never true for `AddCopiesOnly` (`BASE_INSERTED_COUNT` is always 0 there), so every `AddCopiesOnly` file was credited as zero files retrieved, and the unsigned subtraction could underflow and overflow the `BIGINT` bind into `updateRepackRequestsProgress()` whenever a user-provided file was mixed into the same batch.

## Reporting a creation failure doesn't fail the whole expansion

`RepackRequest::addSubrequestsAndUpdateStats()` wraps each file's subrequest creation in its own `try`/`catch`: an exception (including the one `setActiveCopyNumber()` used to throw for every `AddCopiesOnly` file) is caught per file, counted into `failedCreationStats`/`failedArchiveReq`, and logged ("Failed to create subrequest.") rather than propagated. `Scheduler::expandRepackRequest()` itself does not throw because of a single file's failure - the repack request's own `FAILED_TO_CREATE_ARCHIVE_REQ` counter is what reflects it (`cta-admin repack ls`'s equivalent field). A test asserting only that `expandRepackRequest()` doesn't throw will not catch a regression here; it has to check that counter.
