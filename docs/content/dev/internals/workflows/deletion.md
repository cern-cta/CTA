---
title: Deletion and garbage collection architecture
---

!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

!!! warning "Deprecated"
    This page is deprecated and may contain information that is no longer up to date.

# Deletion Internals

Disk-copy eviction belongs to the disk system; this page covers CTA metadata and request cleanup.

## CTA workflow when deleting a file

The deletion of a file from CTA is done by the frontend when the **delete** workflow is triggered. The method `processDELETE()` is called.

The following steps are executed:

* Delete the request from the objectstore (if it exists)
* Move the file to delete into a recycle-bin

### Deletion of the objectstore's ArchiveRequest

If a **delete** is called on a file that is not yet archived to tape, the corresponding ongoing ArchiveRequest will be deleted from the objectstore. The delete notification supplies the ArchiveRequest's objectstore ID when archiving is ongoing.

### Moving of the file to delete into a recycle-bin

If a **delete** is called after the file is successfully archived to tape, the ArchiveFile (`ARCHIVE_FILE`) and the TapeFiles (`TAPE_FILE`) entries will be moved to the [recycle-bin](recycle-bin.md)

## Definitely remove a file from CTA

The only way to definitely remove a file from CTA is to **reclaim** the tape where the file is located.

## Disk-system integration

See [EOS Deletion Workflow](../../guides/integrations/eos/deletion.md) for the EOS-specific protocol and behaviour.

## Failure handling and tests

TODO: Document retries, cancellation, partial completion, recovery after process failure, and the tests covering these paths.
