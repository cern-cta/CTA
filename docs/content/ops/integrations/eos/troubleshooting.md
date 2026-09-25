# Troubleshooting and Repair

Diagnose EOS disk-replica and transfer failures before selecting a recovery procedure.

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

## Collect evidence

Record file and archive identifiers, replica locations, filesystem status, sizes, checksums, and the corresponding CTA request state.

## Unavailable filesystems and missing replicas

Distinguish temporary unavailability from lost replicas. Document supported repair choices and checks before removing or recreating metadata.

## Size and checksum mismatches

Cover wrong sizes, truncated replicas, and checksum failures. Establish which copy is trustworthy before repair; link to [Data Integrity](../../../concepts/data-management/data-integrity.md).

## Verify recovery

Check namespace and catalogue consistency, usable replicas, and successful retrieval. Use [Metadata Consistency and Recovery](metadata-recovery.md) for identity and namespace issues.
