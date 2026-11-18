!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Maintenance Daemon Configuration

Configure background reporting, repack, and scheduler cleanup. See [Maintenance Daemon Concepts](../../../concepts/components/maintenance-daemon.md).

## Routine selection and placement

Available routines depend on the compiled scheduler backend; use the matching [annotated example](#example-configuration) for routine names, descriptions and defaults.

TODO: Document the required routines for each scheduler backend and how to avoid gaps or unintended duplication.

## Intervals and batch sizes

TODO: Document configuration and operational checks for reporting and repack throughput.

## Configuration reference

Use the [annotated examples](#example-configuration) for all settings, defaults and routine selectors.
See the [cta-maintd manual](../../tools/service-manuals/cta-maintd.md) for command-line options and [Service Runtime](service-runtime.md) for validation and feature selection.

## Example configuration

The annotated examples are the complete settings reference.
Choose the variant matching the compiled scheduler backend and replace deployment-specific values.
The installed package provides the matching variant as `/usr/share/doc/cta-maintd/examples/cta-maintd.example.toml`.

??? example "Objectstore scheduler"

    ```toml
    --8<--
    maintd/cta-maintd.example.toml
    --8<--
    ```

??? example "PostgreSQL scheduler"

    ```toml
    --8<--
    maintd/cta-maintd.pg.example.toml
    --8<--
    ```
