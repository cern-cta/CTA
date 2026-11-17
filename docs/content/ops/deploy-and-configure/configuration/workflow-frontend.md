# Workflow Frontend Configuration {#workflow-api-configuration}

Configure the API that receives archive, retrieve, and delete workflow requests from the disk system.

## Service configuration

`cta-workflow-api` reads `/etc/cta/cta-workflow-api.toml` by default.
Use `--config` to select another TOML file.
Configure the gRPC listener and TLS credentials under `[grpc]` and `[grpc.tls]`.
Select exactly one of JWT or mutual TLS using `auth.enabled_methods`, and provide only the selected authentication sections.
Use the [annotated examples](#example-configuration) for all settings and defaults, and [Service Runtime](service-runtime.md) for validation and feature selection.
Restart the service to apply changes.

## Example configuration

The annotated examples are the complete settings reference.
Choose the variant matching the compiled scheduler backend and replace deployment-specific values.
The installed package provides the matching variant as `/usr/share/doc/cta-workflow-api/examples/cta-workflow-api.example.toml`.

??? example "Objectstore scheduler"

    ```toml
    --8<--
    app/cta-workflow-api/cta-workflow-api.example.toml
    --8<--
    ```

??? example "PostgreSQL scheduler"

    ```toml
    --8<--
    app/cta-workflow-api/cta-workflow-api.pg.example.toml
    --8<--
    ```

See [Authentication Configuration](authentication.md) and the [cta-workflow-api command reference](../../tools/service-manuals/cta-workflow-api.md) for authentication guidance and command-line options, respectively.
