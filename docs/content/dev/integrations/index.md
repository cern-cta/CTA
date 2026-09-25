# Disk Buffer Integrations

Develop adapters between a disk system and CTA without making the core CTA documentation depend on that system.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Integration contract

Describe workflow requests, identities, file metadata, transfer endpoints, callbacks, and errors. See [Interfaces and Protocols](../internals/interfaces.md).

## EOS

See [Developing with EOS](eos/environment-setup.md) and the EOS subsections of the archive, retrieve, and delete workflow pages.

## dCache

Reserve the adapter architecture, development setup, and test fixtures here. Operational setup belongs in [Operations](../../ops/integrations/dcache.md).

## EOS protocol and failure testing

Document the current transport, message fields, identity mapping, and compatibility contract. Cover missing replicas, offline filesystems, truncation, checksum mismatches, and file-ID changes as reproducible integration tests. Operator diagnosis belongs in [EOS Troubleshooting and Repair](../../ops/integrations/eos/troubleshooting.md).

## Integrity edge cases

Include checksum propagation and zero-length archive/retrieve cases in integration tests. Keep core CTA behaviour separate from each disk system's policy.
