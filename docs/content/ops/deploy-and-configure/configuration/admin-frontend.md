# Admin Frontend API Configuration {#admin-api-configuration}

Configure the API used by operators and administrative tools, including `cta-admin`.

## Service configuration

`cta-admin-api` reads `/etc/cta/cta-admin-api.toml` by default.
Use `--config` to select another TOML file.
Configure the gRPC listener and TLS credentials under `[grpc]` and `[grpc.tls]`.
Select JWT, Kerberos, or both using `auth.enabled_methods`, and provide only the selected authentication sections.
Use the [annotated examples](#example-configuration) for all settings and defaults, and [Service Runtime](service-runtime.md) for validation and feature selection.
Restart the service to apply changes.

## Client-side configuration

`cta-admin` is configured through the `cta-cli.conf` file:
```ini
grpc.tls.enabled true
grpc.cta_admin_auth_method jwt | krb5
grpc.jwt_token_path /path/to/token
grpc.tls.chain_cert_path /path/to/cert
```
The configuration file is consulted first in order to determine which authentication method to use. If no method is specified, (grpc.cta_admin_auth_method is not set to one of the allowed options, jwt or krb5) then Kerberos authentication will be tried. It is also possible to override the method specified in the configuration through the environment variable `CTA_ADMIN_GRPC_AUTH_METHOD` which can be set to either `jwt` or `krb5`.

## Example configuration

The annotated examples are the complete settings reference.
Choose the variant matching the compiled scheduler backend and replace deployment-specific values.
The installed package provides the matching variant as `/usr/share/doc/cta-admin-api/examples/cta-admin-api.example.toml`.

??? example "Objectstore scheduler"

    ```toml
    --8<--
    app/cta-admin-api/cta-admin-api.example.toml
    --8<--
    ```

??? example "PostgreSQL scheduler"

    ```toml
    --8<--
    app/cta-admin-api/cta-admin-api.pg.example.toml
    --8<--
    ```

See [Authentication Configuration](authentication.md) and the [cta-admin-api command reference](../../tools/service-manuals/cta-admin-api.md) for authentication guidance and command-line options, respectively.
