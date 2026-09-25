# Troubleshooting

Diagnose core CTA failures separately from disk-system failures.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Frontend and authentication

Document connectivity, credentials, and request rejection checks.

## Catalogue and scheduler

Document database connectivity, queue health, and schema checks.

## Tape drives and libraries

Document mount failures, device errors, and tape-state investigation.

## Archive, retrieve, and repack

Document how to locate the failing stage and collect useful logs.

## EOS

Link EOS-specific namespace, buffer, and workflow checks from [EOS Configuration](../integrations/eos/configuration.md).

## dCache

Reserve dCache-specific diagnosis here and in the [integration guide](../integrations/dcache.md).

## Library-specific behaviour

Reserve validated guidance for move timeouts, asynchronous moves, and drive/library state mismatches. Review the historical SpectraLogic behaviour against supported hardware and firmware before documenting remedies.

## EOS replica failures

See [EOS Troubleshooting and Repair](../integrations/eos/troubleshooting.md) for unavailable filesystems, missing replicas, and size/checksum failures.
