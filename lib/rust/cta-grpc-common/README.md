# cta-grpc-common

Transport-level gRPC plumbing shared by the Rust clients of
[CTA](https://gitlab.cern.ch/cta/CTA).

## Contents

| Item | Description |
| ---- | ----------- |
| `EndpointConfig` | Where to connect (an `http://` or `https://` URL), how to authenticate (`JwtAuth`) and which TLS trust material to use. |
| `JwtAuth` | A bearer token. `JwtAuth::new` checks that the bytes are valid UTF-8, and the `Debug` implementation redacts the value. |
| `AuthorizationInterceptor` | A `tonic` interceptor that adds `authorization: Bearer <token>` to every outgoing request. |
| `Error` | Errors raised while configuring or using a connection: I/O, transport, gRPC status, metadata, invalid URI and unsupported scheme. |
| `test_utils` | Helpers that build canned gRPC response streams for unit tests, with no sockets or spawned servers. |
