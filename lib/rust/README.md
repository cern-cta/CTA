# CTA Rust Libraries

The library crates of the CTA Rust workspace. The command-line tools live under
`tools/` instead.

| Crate | Description |
| ----- | ----------- |
| [`cta-grpc-common`](cta-grpc-common/README.md) | Transport-level gRPC plumbing shared by the client crates: `EndpointConfig`, JWT authentication, TLS and test helpers. |
| [`cta-model`](cta-model/README.md) | Business objects of CTA (catalogue and scheduler resources, workflow events) and their protobuf mapping. |
| [`cta-model-macros`](cta-model-macros/README.md) | Procedural macros of `cta-model`, deriving the keys of the resources. |
| [`cta-protobuf`](cta-protobuf/README.md) | Generated Rust protobuf/gRPC bindings for the CTA frontend interface. |
| [`eos-protobuf`](eos-protobuf/README.md) | Generated Rust protobuf/gRPC bindings for the EOS namespace interface. |
| [`cta-client`](cta-client/README.md) | High-level client for the CTA frontend gRPC API. |
| [`eos-client`](eos-client/README.md) | Client for the subset of the EOS namespace gRPC API used by CTA. |
