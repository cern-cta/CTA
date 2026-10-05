# eos-client

Client library for the EOS namespace gRPC API, used by
[CTA](https://gitlab.cern.ch/cta/CTA) tools.

It wraps the generated bindings of
[`eos-protobuf`](https://gitlab.cern.ch/cta/CTA/-/blob/main/lib/rust/eos-protobuf/README.md)
and builds on the transport plumbing of
[`cern-st-grpc`](https://gitlab.cern.ch/cta/CTA/-/blob/main/lib/rust/cern-st-grpc/README.md).

`EosGrpcClient` covers the subset of the EOS RPC service that CTA needs.

## Endpoint registry

`EosEndpointMap` maps a *disk instance* name, as known to the CTA catalogue, to
an `EndpointConfig`, and forwards each call to the matching `EosGrpcClient`. The
connection is opened on demand and is not pooled.

## Authentication

The EOS API carries the bearer token inside the protobuf `authkey` field rather
than an HTTP header. The `with_auth_key_from!` macro fills that field from a
`JwtAuth`.
