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

## Record progress

The scheduler counts retrieved files and bytes from the completed retrieve rows. A user-provided file already in the disk buffer has no retrieve mount ID and contributes only to archive work. Archive job totals come from the rows inserted into the archive queue. The queue transformation and retrieve progress update commit in one transaction.

A subrequest creation failure is logged per file and contributes to the repack request's failure counters. Check `FAILED_TO_CREATE_ARCHIVE_REQ` when testing expansion, since the expansion call can return after recording a per-file failure.
