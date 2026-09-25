# Recommended Access Order (RAO)

Retrieving files in a different order can reduce tape positioning time. Recommended Access Order (RAO) determines an order for a batch of retrievals; it does not replace CTA mount policies or queue scheduling.

## Hardware and software ordering

Hardware RAO asks a capable drive to recommend an order. Software RAO calculates an order in CTA. Linear ordering follows logical file identifiers; random ordering provides a comparison baseline; shortest locate time first (SLTF) uses an estimated positioning cost.

## Tape geometry and positioning

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

Explain wraps, longitudinal position (LPOS), and the distinction between logical file order and physical tape position. Describe why software positioning estimates need media geometry and why an optimised order is scoped to a retrieval batch.

## Related guides

Operators can find prerequisites, configuration, and diagnosis in [RAO configuration](../../ops/configuration/tape-daemon.md#recommended-access-order). The [retrieval lifecycle](../data-management/retrieval.md) explains the surrounding workflow.
