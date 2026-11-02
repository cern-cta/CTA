# Workflow Frontend Configuration {#workflow-api-configuration}

Configure the API that receives archive, retrieve, and delete workflow requests from the disk system.

## Service configuration

The Workflow Frontend is configured via `cta-frontend.conf`.

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

???+ example "cta-workflow-api.example.toml"

    ```toml
    --8<--
    app/cta-workflow-api/cta-workflow-api.example.toml
    --8<--
    ```

See [Authentication Configuration](authentication.md) and the [cta-workflow-api command reference](../../tools/service-manuals/cta-workflow-api.md) for additional options.
