<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
-->

# eos-protobuf

Generated Rust protobuf/gRPC bindings for the EOS interface, used by
[CTA](https://gitlab.cern.ch/cta/CTA).

Almost nothing in this crate is written by hand: `build.rs` runs
[`tonic-prost-build`](https://docs.rs/tonic-prost-build) over the `.proto` files
vendored in `external/eos-grpc-proto/` (`Authentication`, `File`, `Metadata`,
`Recycle`, `Rpc` and `Sched`). Only client stubs are generated
(`build_server(false)`), because CTA acts as an EOS client.

## Modules

| Module | Protobuf package | Contents |
| ------ | ---------------- | -------- |
| `eos::rpc` | `eos.rpc` | Namespace API: `MdRequest`/`MdResponse`, `FileMdProto`, `ContainerMdProto`, insert requests, `EosClient` |
| `eos::console` | `eos.console` | Message types behind the EOS console (`eos ...`) commands |
| `eos::traffic_shaping` | `eos.traffic_shaping` | Scheduling and traffic-shaping messages |

Prefer the higher-level wrappers in [`cta-lib`](https://gitlab.cern.ch/cta/CTA/-/blob/main/tools-rs/cta-lib-rs/README.md), which
handle channel setup, authentication and response streaming.
