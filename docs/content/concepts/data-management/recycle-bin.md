# Recycle Bin

The CTA catalogue retains metadata for deleted or repacked tape copies in a recycle bin. This is separate from any recycle bin or retention policy implemented by the disk system.

## Recovery boundary

Recovery depends on the tape data still being present. Reclaiming a tape removes the opportunity to recover those copies through the recycle bin. Restoring CTA metadata and restoring the disk-system namespace are separate parts of recovery.

See [Recycle Bin and File Recovery](../../ops/administration/file-recovery.md) for operator procedures.

## EOS

EOS namespace restoration and file identifiers are covered separately in the operator procedure. They are not requirements imposed on every disk system.
