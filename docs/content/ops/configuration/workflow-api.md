# Workflow API Configuration

Configure the API that receives archive, retrieve, and delete workflow requests from the disk system.

## Service configuration

The Workflow API is configured via `cta-frontend.conf`.

```ini
# Operation mode: this node is the Workflow Engine (WFE)
cta.operation_mode wfe

# gRPC port
grpc.port 10956

# WFE authentication method: "jwt" or "mtls"
grpc.wfe.auth_method jwt

# TLS
grpc.tls.server_key_path /path/to/server-wfe.key.pem
grpc.tls.server_cert_path /path/to/server-wfe.crt.pem
grpc.tls.chain_cert_path /path/to/ca.crt.pem

# JWKS Endpoint (required for JWT auth)
grpc.jwks.uri file:///etc/cta/jwks.json
# or a remote endpoint:
# grpc.jwks.uri http://auth-keycloak:8080/realms/master/protocol/openid-connect/certs
```

## Example configuration

The current source tree supplies a shared `cta-frontend` example for both APIs. Select the operation mode and authentication settings for this API as shown above; separate service examples can replace this include when the implementation is split.

???+ example "cta-frontend.example.conf"

    ```toml
    --8<--
    frontend/grpc/cta-frontend.example.conf
    --8<--
    ```

See [Authentication Configuration](authentication.md) and the [current command reference](../tools/service-manuals/cta-frontend.md) for additional options.
