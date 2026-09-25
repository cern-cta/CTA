# Media Changer Daemon Configuration

Configure `cta-rmcd`, the service controlling tape-library media moves. See [Media Changer Concepts](../../concepts/components/media-changer-daemon.md).

## Library device and control endpoint

!!! info "Documentation outline"
    Document device discovery, permissions, the RMC endpoint, and the tape daemons that use it. Include mappings for deployments with multiple libraries.

## Service runtime

Use [Service Runtime](service-runtime.md) for shared runtime settings and [Health Checks and Alerts](../monitoring/health-and-alerts.md) for probing.


## Example configuration

???+ example "cta-rmcd.example.toml"

    ```toml
    --8<--
    mediachanger/rmcd/cta-rmcd.example.toml
    --8<--
    ```

## Command reference

See the [cta-rmcd manual](../tools/service-manuals/cta-rmcd.md) for command options.
