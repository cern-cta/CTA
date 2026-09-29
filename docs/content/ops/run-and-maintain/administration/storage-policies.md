!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Storage Policies

Create and maintain the objects described in [Storage Model and Policies](../../../concepts/data-management/storage-model.md).

## Disk instances and virtual organisations

TODO: Document registration, ownership, and operational limits.

## Tape pools, storage classes, and archive routes

TODO: Document initial policy setup and changes to placement or copy count.

## Mount policies and requester rules

TODO: Document scheduling policy assignment and validation.

## Coordinating disk-system metadata

Keep CTA policy changes distinct from disk-system namespace changes.

### EOS storage-class changes

Coordinate EOS metadata, CTA storage classes, and any required repack or copy-count changes. See [cta-ops-eos](../../tools/cta-ops-eos.md#cta-ops-change-storageclass) for the tool-specific procedure.

### EOS disk-instance migration

TODO: Document supported moves between disk instances, including identity mappings and permissions. Verify tool availability before adding commands.
