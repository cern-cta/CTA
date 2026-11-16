!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Maintenance Daemon Configuration

Configure background reporting, repack, and scheduler cleanup. See [Maintenance Daemon Concepts](../../../concepts/components/maintenance-daemon.md).

## Routine selection and placement

TODO: Document the required routines for each scheduler backend and how to avoid gaps or unintended duplication.

## Intervals and batch sizes

TODO: Document configuration and operational checks for reporting and repack throughput.

## Configuration reference

Use the [annotated examples](#example-configuration) for all settings, defaults and routine selectors.
See the [cta-maintd manual](../../tools/service-manuals/cta-maintd.md) for command-line options and [Service Runtime](service-runtime.md) for validation and feature selection.

## Supported routines

The available routines depend on the configured scheduler backend.

### Common routines

disk_report_archive

:   Reports archive job success or failure to the disk system.

disk_report_retrieve

:   Reports retrieve job success or failure to the disk system.

repack_expand

:   Expands repack requests into individual archive and retrieve jobs.

repack_report

:   Handles reporting for repack-generated requests.

### Objectstore-specific routines

queue_cleanup

:   Finds queues marked for cleanup, takes ownership, and moves requests to other queues.

garbage_collect

:   Performs garbage collection on stale agents and objects in the objectstore.

### PostgreSQL-specific routines

Additional queue cleanup routines may be enabled when using the Postgres scheduler backend, including:

* user_active_queue_cleanup
* repack_active_queue_cleanup
* user_pending_queue_cleanup
* repack_pending_queue_cleanup
* scheduler_maintenance_cleanup (also removes expired disk system sleep entries)

## Example configuration

The annotated examples are the complete settings reference.
Choose the variant matching the compiled scheduler backend and replace deployment-specific values.
The installed package provides the matching variant as `/usr/share/doc/cta-maintd/examples/cta-maintd.example.toml`.

### Objectstore scheduler

???+ example "cta-maintd.example.toml"

    ```toml
    --8<--
    maintd/cta-maintd.example.toml
    --8<--
    ```

### PostgreSQL scheduler

???+ example "cta-maintd.pg.example.toml"

    ```toml
    --8<--
    maintd/cta-maintd.pg.example.toml
    --8<--
    ```
