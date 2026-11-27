# PostgreSQL Repack Workflows

Repack expansion selects the copies to write. The PostgreSQL scheduler records the source tape copy on the retrieve job and queues the destination copies after retrieval. See [PostgreSQL Schema](schema.md) for the queue and tracking tables.

## Select the destination copies

`Scheduler::expandRepackRequest()` uses the same rules for both scheduler backends. For each file on the repacked tape, it records the copy numbers to write in `copyNbsToRearchive`.

| Repack type | Copies to write |
| --- | --- |
| `MoveOnly` | The copy on the repacked tape. |
| `AddCopiesOnly` | The missing copies required by the storage class. |
| `MoveAndAddCopies` | The copy on the repacked tape and any missing copies required by the storage class. |

Expansion compares the storage class's required copy count with the file's catalogue copies across all tapes. It uses the archive routes to identify the missing copy numbers and their destination pools. `AddCopiesOnly` skips files that already have enough copies and leaves existing copies in place.

## Queue the retrieve and archive jobs

The retrieve row identifies the existing source copy by VID, file sequence and block ID. For `AddCopiesOnly`, that source copy is outside the destination set. A retrieve subrequest can therefore have an empty internal job list and still needs its normal retry limits. The destination copy numbers and pools are stored separately in `REPACK_REARCHIVE_COPY_NBS` and `REPACK_REARCHIVE_TAPE_POOLS`.

`transformJobBatchToArchive()` takes completed retrieve rows and inserts one job per destination copy into `REPACK_ARCHIVE_PENDING_QUEUE`. It uses the source copy's own number and pool only when that copy is in the destination set. The other destinations use their paired copy numbers and pools. For example, reading copy 1 to add copies 2 and 3 queues two archive jobs, each with `REQUEST_JOB_COUNT` set to two.

## Complete a multi-copy request

`updateRepackJobSuccess()` settles a reported archive job against its siblings. The sibling group is `(REPACK_REQUEST_ID, ARCHIVE_FILE_ID)`, not the archive file ID alone: the same file can be in two repack requests at once when two tapes each hold one of its copies, and those rows are not siblings.

A reported job moves to `AJS_ToReportToRepackForSuccess` on its own. Only once every copy in the group has settled do all of them move together to `ReadyForDeletion`, which is what allows the source file to be deleted from the disk buffer. A sibling still being written is left untouched until then, so it is never marked successful before its own write finishes.

!!! warning "One job per sibling group per reported batch"

    A batch passed to `updateRepackJobSuccess()` must contain at most one job per `(REPACK_REQUEST_ID, ARCHIVE_FILE_ID)` group. The completeness check counts each already-settled sibling once per target row in the batch, so two targets from the same group inflate the count, the group is never recognised as complete, and the repack request can never finish.

    This holds today because a batch comes from one archive mount, a mount serves one tape pool, and two copies of the same file can never share a tape pool — a storage class's archive routes must map distinct copy numbers to distinct tape pools, and expansion is rejected otherwise. Relaxing that routing rule, or batching jobs across tape pools, requires deduplicating the sibling count first.

!!! note "Limits of the current settlement"

    The batch invariant above only rules out duplicate targets within one reported batch. It does not serialize two mounts settling different siblings of the same group concurrently: each incomplete-group settlement updates only its own row, so the two transactions never block each other, each can read the other's copy as still transferring, and both park without the group ever reaching `ReadyForDeletion`. The completeness count is also success-only — a group containing a terminally failed sibling is never recognised as settled by the success path, and the failed-archive sweep deletes its jobs' source buffer files without consulting sibling state. The broader settlement rework addressing these limits (locking the whole sibling group up front, and failing or succeeding a group together) is prepared separately on the failure-batching branch.

## Record progress

The scheduler counts retrieved files and bytes from the completed retrieve rows. A user-provided file already in the disk buffer has no retrieve mount ID and contributes only to archive work. Archive job totals come from the rows inserted into the archive queue. The queue transformation and retrieve progress update commit in one transaction.

Because that one transaction also inserts archive queue rows, it already holds `FOR KEY SHARE` locks on the tracking row through their foreign keys. The progress statement therefore locks the tracking row `FOR NO KEY UPDATE`, which still serializes concurrent progress updates but does not conflict with those foreign-key locks. `FOR UPDATE` does conflict with them, and would let two workers on the same repack request deadlock.

A subrequest creation failure is logged per file and contributes to the repack request's failure counters. Check `FAILED_TO_CREATE_ARCHIVE_REQ` when testing expansion, since the expansion call can return after recording a per-file failure.
