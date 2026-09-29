!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Troubleshooting

Diagnose core CTA failures separately from disk-system failures.

| I need to investigate… | Start here |
| --- | --- |
| Failed or stalled archive/retrieve requests | [Failed & Stuck Requests](failed-requests.md) |
| Startup probing or stuck tape media | [Tapes, Drives & Libraries](../run-and-maintain/administration/tapes-and-drives.md#startup-probing-and-stuck-media) |
| A partially failed repack | [Repack recovery](../run-and-maintain/administration/repack.md#recovering-from-partial-failures) |

## Frontend and authentication

TODO: Document connectivity, credentials, and request rejection checks.

## Catalogue and scheduler

TODO: Document database connectivity, queue health, and schema checks.

## Tape drives and libraries

TODO: Document mount failures, device errors, and tape-state investigation.

## Archive, retrieve, and repack

TODO: Document how to locate the failing stage and collect useful logs.

## Library-specific behaviour

TODO: Reserve validated guidance for move timeouts, asynchronous moves, and drive/library state mismatches. Review the historical SpectraLogic behaviour against supported hardware and firmware before documenting remedies.

## Tool installation failures

See [operator-tool installation troubleshooting](../tools/installation-and-configuration.md#troubleshooting) for dependency installation failures.
