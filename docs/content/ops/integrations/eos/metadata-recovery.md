# Metadata Consistency and Recovery

Operator procedures for keeping EOS namespace metadata and CTA catalogue records consistent.

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

## Metadata ownership and reconciliation

Identify authoritative fields and compare namespace, archive identifiers, tape copies, and disk replicas. Separate implemented checks from historical reconciliation proposals.

## Recovery scenarios

Cover a deleted namespace entry, a retained entry with an unchanged file ID, and a retained entry with a changed file ID. Specify prerequisites, supported tools, ordering, and post-recovery checks. Account for failures between changes to the two systems. See [Recycle Bin & File Recovery](../../administration/file-recovery.md).

## Namespace injection and instance migration

Document supported namespace reconstruction and moves between disk instances, including identity mappings and permissions. Verify tool availability before adding commands.

## Storage-class changes

Coordinate EOS metadata, CTA storage classes, and any required repack or copy-count changes. Link to [Storage Policies](../../administration/storage-policies.md).

## Conversion and file identifiers

Explain how EOS conversion can affect file IDs and the implications for catalogue mappings, reconciliation, and recovery.
