# Admin API Configuration

Configure the API used by operators and administrative tools, including `cta-admin`.

## Service configuration

The Admin API is configured via `cta-frontend.conf`.

```ini
# Operation mode: this node serves admin commands
cta.operation_mode admin_all

# gRPC port
grpc.port 10956

# Admin authentication methods (space-separated list): "jwt", "kerberos"
grpc.admin.auth_methods jwt kerberos

# TLS
grpc.tls.server_key_path /path/to/server-admin.key.pem
grpc.tls.server_cert_path /path/to/server-admin.crt.pem
grpc.tls.chain_cert_path /path/to/ca.crt.pem

# JWKS Endpoint (required for JWT auth)
grpc.jwks.uri file:///etc/cta/jwks.json

# Kerberos (required when kerberos is in auth_methods)
grpc.keytab /etc/cta/cta-frontend.krb5.keytab
grpc.service_principal cta/cta-frontend-admin@REALM
```

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

The current source tree supplies a shared `cta-frontend` example for both APIs. Select the operation mode and authentication settings for this API as shown above; separate service examples can replace this include when the implementation is split.

???+ example "cta-frontend.example.conf"

    ```toml
    --8<--
    frontend/grpc/cta-frontend.example.conf
    --8<--
    ```

See [Authentication Configuration](authentication.md) and the [current command reference](../tools/service-manuals/cta-frontend.md) for additional options.
