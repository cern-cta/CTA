---
title: File Workflows
---

# File Workflows

CTA's file workflows register files, create tape copies, retrieve them to disk, and remove them from the active catalogue. The disk system owns the namespace and disk replicas; CTA manages tape copies and the work needed to read or write them.

A typical file is registered, archived, and later retrieved when needed. Its disk replica can be removed and recreated without changing its tape copies. Deleting the file from CTA is a separate operation from evicting a disk replica.

## Workflow events

The disk system submits these events to the [Workflow Frontend](../components/workflow-api.md). Both EOS and dCache integrations use the event names below. The names reflect events and requests in the file's lifecycle on disk; the corresponding CTA operations describe what CTA does in response.

For example, `CREATE` means that a file has been created in the disk namespace, before its contents have been written. `CLOSEW` means **close after writing**: the file is fully written and ready to be archived.

| Event | CTA operation | Meaning in CTA |
| --- | --- | --- |
| `CREATE` | **Registration** | Validates the storage class and archive routes and allocates an archive file ID for subsequent requests. No file data is transferred. |
| `CLOSEW` | **Archival** | Queues the required tape copies once the file is ready in the disk buffer. Each copy is queued for its destination tape pool. |
| `PREPARE` | **Retrieval** | Requests that the file be made available on disk. CTA queues a read from a tape holding a copy of the file, with the disk buffer as the destination. |
| `ABORT_PREPARE` | **Retrieval cancellation** | Requests cancellation of a previously submitted retrieval. It does not delete the tape copy. |
| `DELETE` | **Deletion** | Removes the file's active catalogue metadata, retaining tape-copy metadata in the recycle bin where applicable, and cancels pending archival work. It does not physically erase the bytes on tape. |

## Acceptance and completion

Registration returns an archive file ID synchronously. Acceptance of an archival or retrieval request means the work has been queued, not that its data transfer has finished. [Scheduling](scheduling.md) determines when a tape daemon can serve it.

After the transfer, completion or failure reaches the disk system through the integration's reporting or transfer-completion mechanism. The disk system then updates its own state. See [Disk Buffer](../components/disk-buffer.md) for this division of responsibilities.

## Individual workflows

- [Archival](archival.md): writing the required tape copies and recording their locations.
- [Retrieval](retrieval.md): restoring a tape copy to the disk buffer for access by clients.
- [Deletion](deletion.md): removing active catalogue entries, distinct from removing disk replicas.
- [Repack](repack.md): an operator-initiated workflow that moves tape copies to new tapes, for example when retiring media.
- [Recycle Bin](recycle-bin.md): metadata retained after deletion to support recovery while the data remains on tape.

The individual workflow pages describe integration-specific behaviour under the relevant disk-system headings.
