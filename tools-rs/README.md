<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
-->

# tools-rs — CTA tools written in Rust

A Cargo workspace with the (experimental) Rust parts of [CTA](https://gitlab.cern.ch/cta/CTA):
generated gRPC bindings for the CTA frontend and for EOS, a client library on
top of them, and tools built on it (currently only `cta-restore-files`).

| Crate | Directory | Purpose |
| ----- | --------- | ------- |
| `cta-protobuf` | `cta-protobuf-rs/` | Generated bindings for the CTA frontend protobuf interface (`xrootd-ssi-protobuf-interface`) |
| `eos-protobuf` | `eos-protobuf-rs/` | Generated bindings for the EOS protobuf interface (vendored in `external/eos-grpc-proto`) |
| `cta-lib` | `cta-lib-rs/` | Client library: endpoint/TLS/JWT setup, typed CTA and EOS clients |
| `cta-restore-files` | `cta-restore-files/` | Tool to restore deleted tape files in the CTA catalogue and the EOS namespace |


## Toolchain

`git submodule init` is required before use (should have been executed at repo config time).
`cta-protobuf` compiles the `.proto` files of the surrounding CTA checkout
(`lib/protobuf/external/xrootd-ssi-protobuf-interface/`), so the submodules of
the repository must be initialised before building:

`protoc` is provided by `tonic-prost-build`, so no system protobuf compiler is
needed.

## Build, test, document

```bash
cargo build --workspace                 # debug build of everything
cargo build --workspace --release       # optimized build (see [profile.release])
cargo clippy --workspace --all-targets  # lints (workspace lint policy applies)
cargo test --workspace                  # unit tests and doctests
cargo fmt --all                         # formatting
cargo doc --workspace --no-deps --open  # API documentation
```

`Cargo.lock` is committed, since the workspace ships binaries. Per-crate lockfiles
are ignored by cargo inside a workspace and must not be created.

## Running the tools

See the crate-level documentation (`cargo doc`) or `--help` of each tool:

```bash
cargo run -p cta-restore-files -- --help
```

Logging is configured through `RUST_LOG`, e.g. `RUST_LOG=debug`.
