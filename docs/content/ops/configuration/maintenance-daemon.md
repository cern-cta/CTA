# Maintenance Daemon Configuration

Configure background reporting, repack, and scheduler cleanup. See [Maintenance Daemon Concepts](../../concepts/components/maintenance-daemon.md).

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Routine selection and placement

Document the required routines for each scheduler backend and how to avoid gaps or unintended duplication.

## Intervals and batch sizes

Document configuration and operational checks for reporting and repack throughput.

## Configuration reference

See the [cta-maintd reference](../tools/service-manuals/cta-maintd.md) and [example configuration below](#example-configuration).

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
