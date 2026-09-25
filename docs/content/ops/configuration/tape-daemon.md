# Tape Daemon Configuration

Configure `cta-taped` for each tape drive. See [Tape Server Setup](../deployment/tape-servers.md) for host and hardware preparation.

## Drive and library mapping

Use [stable drive paths](../deployment/udev-rules.md) rather than numbered `/dev/nstX` paths in production configurations.

!!! info "Documentation outline"
    Document drive identities, device paths, logical libraries, media-changer connectivity, and the relationship to TPCONFIG.

## Catalogue, scheduler, and transfers

!!! info "Documentation outline"
    Document connections to the [catalogue](catalogue.md) and [scheduler backend](scheduler.md), transfer settings, and resource limits. Keep disk-system-specific settings in the integration guides.


## Example configuration

???+ example "cta-taped.example.conf"

    ```toml
    --8<--
    taped/daemon/cta-taped.example.conf
    --8<--
    ```

## Recommended Access Order

Use RAO to optimise the order in which the tape daemon retrieves files. See [RAO concepts](../../concepts/tape/rao.md) for the distinction between hardware and software ordering.

### Enablement and algorithm selection

The [tape-daemon option reference](../tools/service-manuals/cta-taped.md#recommended-access-order-rao-options) documents `UseRAO`, `RAOLTOAlgorithm`, and `RAOLTOAlgorithmOptions`. Keep configuration syntax and defaults aligned with that reference and the [example configuration above](#example-configuration).

The reference describes hardware RAO for IBM enterprise drives and IBM LTO generation 9 and later. Its software RAO guidance covers LTO-8. Check the installed drive's capabilities when commissioning RAO.

For software RAO, `linear` and `random` provide comparison baselines; `sltf` is the production algorithm described by the reference. SLTF requires `nbwraps`, `minlpos`, and `maxlpos` in the catalogue media type, managed through `cta-admin mediatype`. The documented cost heuristic is `cost_heuristic_name:cta`.

### Verify ordering and diagnose fallback

The RAO manager logs `executedRAOAlgorithm` after successful ordering. It falls back to linear ordering if algorithm creation or execution throws an exception. Check the accompanying warning and configured algorithm, options, media geometry, and drive capability before treating fallback as expected operation.

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

Add a commissioning checklist, representative success/fallback logs, and a controlled comparison of positioning time and retrieval throughput. Explain which changes require service restart and how to restore the previous configuration.

## Command reference

See the [cta-taped manual](../tools/service-manuals/cta-taped.md) for the complete option reference.
