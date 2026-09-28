!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Maintenance Daemon Configuration

Configure background reporting, repack, and scheduler cleanup. See [Maintenance Daemon Concepts](../../../concepts/components/maintenance-daemon.md).

## Routine selection and placement

TODO: Document the required routines for each scheduler backend and how to avoid gaps or unintended duplication.

## Intervals and batch sizes

TODO: Document configuration and operational checks for reporting and repack throughput.

## Configuration reference

See the [cta-maintd reference](../../tools/service-manuals/cta-maintd.md) and [example configuration below](#example-configuration).

## Supported Routines

### Objectstore

- `DiskReportArchiveRoutine`
- `DiskReportRetrieveRoutine`
- `RepackExpandRoutine`
- `RepackReportRoutine`
- `GarbageCollectRoutine`
- `QueueCleanupRoutine`

### Postgres

- `DiskReportArchiveRoutine`
- `DiskReportRetrieveRoutine`
- `RepackExpandRoutine`
- `RepackReportRoutine`


## Example configuration

???+ example "cta-maintd.example.toml"

    ```toml
    --8<--
    maintd/cta-maintd.example.toml
    --8<--
    ```
