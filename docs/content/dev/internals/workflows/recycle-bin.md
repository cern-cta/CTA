!!! warning "Deprecated"
    This page is deprecated and may contain information that is no longer up to date.

# The recycle-bin

A recycle-bin has been implemented in CTA to cover the two following use cases:

- When the disk system requests deletion of a file, we need to log this deletion for future recovery if necessary
- When an operator repacks a tape, the files located on the source tape will be moved to the destination tape(s). We want to keep a trace of the files that were on the source tape for future recovery if necessary

## The structure of the recycle-bin

The recycle-bin is just a table in the CTA Catalogue database. Its name is `FILE_RECYCLE_LOG` and each entry corresponds to a deleted or repacked **tape file**. For each entry on this table, the deleted or repacked **tape file** will be inserted as well as the associated **archive file** information.

In addition to the **tape file** fields and the **archive file** fields, the following columns have been added the the `FILE_RECYCLE_LOG` table in order to have more information about the deleted/repacked files:

- the `DISK_FILE_ID_WHEN_DELETED` column stores the diskFileId the file had when it was deleted (supplied by the disk system)
- the `DISK_FILE_PATH` column stores the path of the file given by the disk system when the file is deleted
- the `REASON_LOG` column stores the reason why the file has been put to the recycle-bin (repack or user deletion)
- the `RECYCLE_LOG_TIME` column to store the time the file has been put to the recycle-bin


## State changes

Deletion and repack remove active tape-file entries while retaining recovery metadata. Operator examples are under [Recycle Bin and File Recovery](../../../ops/administration/file-recovery.md).

## EOS

EOS deletion notifications supply disk identifiers and paths. EOS command examples are in the operator procedure.
