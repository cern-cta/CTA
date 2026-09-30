<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
-->

# eos-protobuf

Generated Rust protobuf/gRPC bindings for the EOS interface, used by
[CTA](https://gitlab.cern.ch/cta/CTA).

Almost nothing in this crate is written by hand: `build.rs` runs
[`tonic-prost-build`](https://docs.rs/tonic-prost-build) over the `.proto`
files vendored in `eos-grpc-proto`. Only client stubs are generated
(`build_server(false)`), because CTA acts as an EOS client.
