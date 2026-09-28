# Recycle Bin

The CTA recycle bin retains catalogue metadata for tape copies removed from the active catalogue. It is also called the **file recycle log**. It stores references to data still on tape, not an additional copy of that data, and is separate from any recycle bin provided by the disk system.

## What is retained

Each entry describes a former tape copy: its archive file ID, disk identity, size and checksums, storage class, copy number, tape VID, and position on tape. Entries also record when and why the copy was removed.

Entries can be created by:

- **File deletion:** metadata for the file's recorded tape copies is retained when its active catalogue records are removed.
- **Repack:** an old copy is retained in the recycle log when it is replaced by a newly written copy. The file itself can remain active at its new location.
- **Removal of an individual tape copy:** the removed copy's metadata is retained even when other active copies remain.

Recycle-bin entries are not active copies and are not used for normal retrieval. Their presence does not imply that the disk namespace entry still exists or that the tape remains readable.

## Recovery boundary

Recovery requires both the retained metadata and the corresponding readable tape data. Restoring an entry reinstates active catalogue metadata; it does not itself read the file back to disk.

If the disk namespace entry was also deleted, it must be restored or recreated and its identity coordinated with CTA. Restoring CTA's catalogue alone does not restore the client's path, permissions, or disk replica. Conversely, a disk-system recycle bin does not by itself restore CTA's tape-copy records.

The recycle bin is therefore a recovery mechanism, not a substitute for catalogue backups or additional tape copies. See [Recycle Bin and File Recovery](../../ops/troubleshooting-and-recovery/file-recovery.md) for operator procedures, including EOS namespace coordination.

## Reclamation and reuse

Reclaiming a tape deletes its recycle-bin entries and resets its catalogue counters for reuse. CTA requires that no active tape copies remain on the tape and checks the other [reclamation conditions](../tape/lifecycle.md), including the recycle-log quarantine period.

After reclamation, those copies can no longer be restored through the recycle bin. Reclamation itself is a catalogue operation, but preparing a tape for reuse commonly also involves relabelling it. Writing new labels at the beginning of the tape establishes a new logical end of data (EOD), so the old records beyond it are no longer accessible through normal tape reads. This does not require physically overwriting all of the previous data.

The distinction is between physical remnants and readable records: bytes that may remain on the medium after relabelling do not constitute recoverable CTA copies. Relabelling is destructive, even though it is not a full-media secure erase. See [Media Initialisation](../../ops/run-and-maintain/administration/media-initialisation.md) for the labelling workflow.

See [Deletion](deletion.md) for the distinction between file deletion, disk-replica eviction, and tape reclamation, and [Repack](repack.md) for replacing active copies before a tape is reused.
