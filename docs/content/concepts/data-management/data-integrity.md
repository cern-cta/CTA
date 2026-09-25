# Data Integrity

Describe how file identity, size, and checksums are preserved across archival, tape storage, retrieval, and repack.

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

## Checksum responsibilities

Identify which component supplies, stores, and verifies checksums at each boundary. Distinguish file checksums from tape-format and drive-level protection.

## Verification and mismatches

Explain how verification failures relate to copy health, request failure, and operator intervention. Link the operational process to [tape verification](../../ops/tools/tape-verification.md).

## Zero-length files

Define the core CTA handling of empty files, including catalogue records and archive/retrieve requests. Keep disk-system policy separate from tape behaviour.

## EOS

Describe EOS checksum policy and empty-file handling separately. Explain the EOS/CTA consistency boundary and link to [metadata consistency and recovery](../../ops/integrations/eos/metadata-recovery.md).
