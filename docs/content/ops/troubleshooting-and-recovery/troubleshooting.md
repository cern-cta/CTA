!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Troubleshooting

Diagnose core CTA failures separately from disk-system failures.

## Frontend and authentication

TODO: Document connectivity, credentials, and request rejection checks.

## Catalogue and scheduler

TODO: Document database connectivity, queue health, and schema checks.

## Tape drives and libraries

TODO: Document mount failures, device errors, and tape-state investigation.

## Archive, retrieve, and repack

TODO: Document how to locate the failing stage and collect useful logs.

## EOS

Link EOS-specific namespace, buffer, and workflow checks from [EOS Configuration](../deploy-and-configure/integrations/eos/configuration.md).

## dCache

TODO: Reserve dCache-specific diagnosis here and in the [integration guide](../deploy-and-configure/integrations/dcache.md).

## Library-specific behaviour

TODO: Reserve validated guidance for move timeouts, asynchronous moves, and drive/library state mismatches. Review the historical SpectraLogic behaviour against supported hardware and firmware before documenting remedies.

## EOS replica failures

See [EOS Troubleshooting and Repair](../deploy-and-configure/integrations/eos/troubleshooting.md) for unavailable filesystems, missing replicas, and size/checksum failures. For namespace/catalogue consistency and metadata recovery, see [EOS Metadata Consistency & Recovery](../deploy-and-configure/integrations/eos/metadata-recovery.md).
