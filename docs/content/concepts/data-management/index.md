---
title: File Workflows
---

# File Workflows

CTA's file workflows register files, create tape copies, retrieve them to disk, and remove them from the active catalogue. The disk system owns the namespace and disk replicas; CTA manages tape copies and the work needed to read or write them.

A typical file is registered, archived, and later retrieved when needed. Its disk replica can be removed and recreated without changing its tape copies. Deleting the file from CTA is a separate operation from evicting a disk replica.

## Workflow events

The disk system submits these events to the [Workflow Frontend](../components/workflow-frontend.md). Both EOS and dCache integrations use the event names below. The names reflect events and requests in the file's lifecycle on disk; the corresponding CTA operations describe what CTA does in response.

For example, `CREATE` means that a file has been created in the disk namespace, before its contents have been written. `CLOSEW` means **close after writing**: the file is fully written and ready to be archived.

| Event | CTA response |
| --- | --- |
| `CREATE` | Validates registration information and allocates an archive file ID. It does not transfer data or prove that a tape copy exists. |
| `CLOSEW` (close after writing) | Queues copies of a completed, non-empty file for the destination pools selected by its archive routes. Permitted empty files are acknowledged without a tape write. |
| `PREPARE` | Selects a recorded tape copy and queues retrieval to the supplied disk destination. |
| `ABORT_PREPARE` | Requests cancellation of retrieval; leaves the tape copy intact. |
| `DELETE` | Removes active archive metadata, retains recorded copy metadata in the recycle bin, and handles identified pending archive work. It does not erase tape bytes. |

See [Disk System Integration](../../ops/deploy-and-configure/integrations/index.md) for the supported integration and protocol requirements.

## Acceptance and completion

Registration returns an archive file ID synchronously. For non-empty archival and for retrieval, acceptance means the work has been queued, not that its data transfer has finished. Permitted [empty-file submissions](data-integrity.md#zero-length-files) return success without queueing a tape write or producing the usual transfer-completion report. [Scheduling](scheduling.md) determines when a tape daemon can serve it.

After the transfer, completion or failure reaches the disk system through the integration's reporting or transfer-completion mechanism. The disk system then updates its own state. See [Disk System](../components/disk-system.md) for this division of responsibilities.

## Individual workflows

- [Archival](archival.md): create the required tape copies.
- [Retrieval](retrieval.md): make a disk replica from a tape copy.
- [Deletion](deletion.md): remove active catalogue entries.
- [Repack](repack.md): replace or add tape copies through an operator-initiated workflow.
- [Recycle Bin](recycle-bin.md): understand retained copy metadata and recovery limits.
- [Data Integrity](data-integrity.md): understand transfer checks and their guarantees.
