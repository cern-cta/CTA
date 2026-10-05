# CTA Rust Libraries

The library crates of the CTA Rust workspace. The command-line tools live under
`tools/` instead.

| Crate | Description |
| ----- | ----------- |
| [`cern-st-grpc`](cern-st-grpc/README.md) | Transport-level gRPC plumbing shared by the client crates: `EndpointConfig`, JWT authentication, TLS and test helpers. |
| [`cta-protobuf`](cta-protobuf/README.md) | Generated Rust protobuf/gRPC bindings for the CTA frontend interface. |
| [`eos-protobuf`](eos-protobuf/README.md) | Generated Rust protobuf/gRPC bindings for the EOS namespace interface. |
| [`cta-client`](cta-client/README.md) | High-level client for the CTA frontend gRPC API. |
| [`eos-client`](eos-client/README.md) | Client for the subset of the EOS namespace gRPC API used by CTA. |
