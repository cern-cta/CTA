---
title: File deletion
---

# Deletion

Deleting a file from CTA removes its active catalogue metadata, so its tape copies are no longer available for normal retrieval. Metadata for the removed copies is retained in the [Recycle Bin](recycle-bin.md). The deletion does not overwrite the file's bytes on tape.

## Deletion, eviction, and reclamation

These operations affect different parts of the storage system:

| Operation | What changes | What remains |
| --- | --- | --- |
| **Disk-replica eviction** | The disk system removes a disk replica. | The namespace entry and tape copies remain; the file can be retrieved again. |
| **File deletion** | The disk system removes the namespace entry and requests removal of CTA's active file metadata. | Tape-copy metadata is retained in CTA's recycle bin, and the bytes remain on tape. |
| **Tape reclamation** | CTA removes a tape's recycle-bin entries and resets its catalogue counters to allow reuse, once reclamation conditions are met. | Reclamation itself changes catalogue metadata. Relabelling or writing from the beginning establishes a new end of data, making old records beyond it inaccessible to normal reads even if physical remnants remain. |

Deleting individual files does not immediately free usable space on tape. To reuse a tape that still contains wanted files, those copies must first be moved elsewhere, typically through [Repack](repack.md). See [Tape Lifecycle](../tape/lifecycle.md) for reclamation conditions.

## Deletion workflow

1. **Submit the deletion (`DELETE`).** The disk system notifies the [Workflow Frontend](../components/workflow-frontend.md) that the file is being deleted. Namespace removal remains the disk system's responsibility.
2. **Handle pending archival.** CTA cancels pending archive work when the request identifies it. Deletion can overlap with transfers already in progress, so the integration must also handle failures of subsequent data or metadata operations.
3. **Remove active catalogue entries.** CTA retains the recorded tape-copy metadata in the recycle bin and removes the active tape-file and archive-file records. A file that has not yet produced a recorded tape copy has no such copy to recover.

After deletion, recovery requires restoring metadata before the copy can be retrieved normally. Deletion is therefore different from retrieval cancellation (`ABORT_PREPARE`), which leaves the active tape-copy records intact.

## Consistency and recovery

The disk namespace and CTA catalogue are separate systems. A failure between their updates can leave one side referring to a file that the other no longer considers active. Such discrepancies need investigation and reconciliation; deletion does not guarantee automatic repair across both systems.

Restoring CTA metadata alone does not recreate the disk namespace entry or a disk replica. The [Recycle Bin](recycle-bin.md#recovery-boundary) explains what must remain available for recovery and how tape reclamation ends that recovery path.

## EOS example

### Evicting disk replicas

EOS eviction removes disk data while preserving the namespace entry and tape copies. Explicit client eviction and background garbage collection both serve this purpose; neither is a CTA `DELETE` request. EOS checks whether a replica is eligible for eviction, including whether its data is safely archived and whether it is still needed.

See [Retrieval](retrieval.md#shared-requests-and-disk-replica-retention) for shared client requests and [EOS Buffer Cleanup](../../ops/deploy-and-configure/integrations/eos/buffer-cleanup.md) for the garbage-collection mechanisms and operational settings.

### Deleting the file

Removing the file from the EOS namespace, for example with `eos rm`, also triggers a deletion request to CTA. The integration must avoid leaving EOS advertising a valid tape copy after its active CTA record has been removed.

If EOS removes its namespace reference but CTA deletion fails, tape data and catalogue records can remain without a corresponding EOS file. This inconsistency must be logged and reconciled; it should not be described as an automatic future cleanup. See [Metadata Consistency & Recovery](../../ops/deploy-and-configure/integrations/eos/metadata-recovery.md).

EOS namespace recovery and CTA recycle-bin restoration are separate operations. The coordinated procedure belongs under [Recycle Bin and File Recovery](../../ops/troubleshooting-and-recovery/file-recovery.md).
