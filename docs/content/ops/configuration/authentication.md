# Authentication Configuration

Configure credentials for each [CTA interface](../../concepts/components/authentication.md). Disk-system configuration belongs to the corresponding [disk buffer integration](../integrations/index.md).

## JWT Authentication


Both the WFE and Admin API frontends support JWT (JSON Web Token) authentication, using JWKS (JSON Web Key Set) for public key validation.
The client should attach the JWT token to the gRPC call credentials. In general, client authentication is expected to work as follows:

1. **Obtain JWT Token**: Get a valid JWT token from your identity provider
2. **Include Authorization Header**: Add token to gRPC metadata:
   ```
   Authorization: Bearer <jwt-token>
   ```
3. **Make gRPC Calls**: All workflow operations (Create, Archive, Retrieve, etc.)

Disk-system clients must supply credentials for their registered disk instance.

### Expected Token Format

The tokens are expected to be valid [JWTs](https://www.jwt.io/introduction#what-is-json-web-token).

The subject field (`"sub"` claim) must contain:

- For **workflow events**: the name of the disk instance from which the request was submitted.
- For **admin commands**: the name of the authorized admin user.

The decoded header might look like this:
```json
{
  "alg": "RS256",
  "typ": "JWT",
  "kid": "he5OTFOKWnYEi4QKSxsIgdLb-0dncPMhFQNrKpZAdi8"
}
```

For workflow events, the decoded payload of a JWT might look like this:
```json
{
  "exp": 1776449678,
  "iat": 1776413678,
  "sub": "disk-instance",
  "typ": "Bearer",
  ...
}
```

For admin commands, the decoded payload might look like this:
```json
{
  "exp": 1776452706,
  "iat": 1776416706,
  "sub": "ctaadmin1",
  "typ": "Bearer",
  ...
}
```

The frontend validates the following claims:
- `kid`: The JWT must have a header claim that matches the key in the `grpc.jwks.uri` that signed the token
- `exp`: Must be after the current time in UTC
- `sub`: Must match one of the entries in the list of authorized admin users (`cta-admin admin ls`) for admin commands, or must match the instance name of the disk instance who sent the workflow event
- `alg`: Is expected to be RS256

### Optional JWKS Cache Settings

A cache for the public keys is maintained so that the public keys used to validate tokens do not have to be looked up for every request.

```ini
# Cache refresh interval (default: 600 seconds)
grpc.jwks.cache.refresh_interval_secs 600

# Public key timeout (default: no expiration if not set)
grpc.jwks.cache.timeout_secs 3600
```



## mTLS Authentication (WFE only)


The WFE frontend supports mutual TLS (mTLS) as an alternative to JWT for authenticating workflow events.
In mTLS mode, the disk-system client presents its own certificate during the TLS handshake.
The frontend maps the client certificate's Common Name (CN) to a disk instance name using a TOML mapping file (`mtls-map.toml`).

Example `mtls-map.toml`:
```toml
[aliases]
disk-instance = ["disk.example.ch", "disk.example"]
```

This allows a single logical disk instance (`disk-instance`) to be represented by certificates with different CNs.


## Kerberos (Admin API)

Configure the frontend service principal and keytab, and obtain a ticket for the configured realm using `kinit`. Configure the admin client as described in [Admin API client configuration](admin-api.md#client-side-configuration).

## Disk transfers

The tape daemon needs credentials accepted by the selected disk system.

### EOS

Document SSS key distribution and rotation here; see [EOS Configuration](../integrations/eos/configuration.md).
