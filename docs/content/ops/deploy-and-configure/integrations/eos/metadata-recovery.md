!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Metadata Consistency and Recovery

Operator procedures for keeping EOS namespace metadata and CTA catalogue records consistent.

## Metadata ownership and reconciliation

Identify authoritative fields and compare namespace, archive identifiers, tape copies, and disk replicas. Separate implemented checks from historical reconciliation proposals.

## Recovery scenarios

TODO: Cover a deleted namespace entry, a retained entry with an unchanged file ID, and a retained entry with a changed file ID. Specify prerequisites, supported tools, ordering, and post-recovery checks. Account for failures between changes to the two systems. See [Recycle Bin & File Recovery](../../../troubleshooting-and-recovery/file-recovery.md).

## Namespace injection and instance migration

TODO: Document supported namespace reconstruction and moves between disk instances, including identity mappings and permissions. Verify tool availability before adding commands.

## Storage-class changes

Coordinate EOS metadata, CTA storage classes, and any required repack or copy-count changes. Link to [Storage Policies](../../../run-and-maintain/administration/storage-policies.md).

## Conversion and file identifiers

TODO: Explain how EOS conversion can affect file IDs and the implications for catalogue mappings, reconciliation, and recovery.

## Historical recovery requirements

!!! warning "Requires review"

    These retained design notes describe recovery use cases, not a validated recovery procedure.

### Recovery use cases


There are two main use cases for files recovering. Each use case has sub-use-cases.
* Recover files that have been deleted by a user or by EOS
  * By ArchiveFileId, by DiskFileId, by file path
* Recover files that have been repacked
  * Rollback a repack on a repacked tape
* For later: recover bad copies of a file
  * We would like to be able to recover a copy of a file from another one. Example : copyNb 1 of a file is broken we would like to restore it by using the copyNb 2.

For each use-case, we would like to be able to list what are the candidates to be recovered.
