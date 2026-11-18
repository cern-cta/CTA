<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
-->

# cta-protobuf

Generated Rust protobuf/gRPC bindings for the [CTA](https://gitlab.cern.ch/cta/CTA)
frontend interface.

Nothing here is written by hand: the bindings are generated at build time from the
`.proto` files of the `xrootd-ssi-protobuf-interface` submodule (`lib/protobuf/external/` in
the CTA repository), so they always match the C++ frontend of the same checkout.

Initialise the submodules before building:

```bash
git submodule update --init --recursive
```

## A note about `build.rs`

`build.rs` makes almost all of the work in this crate. It:

1. **Generates the bindings** with [`tonic-prost-build`](https://docs.rs/tonic-prost-build):
   a Rust type per message, and the gRPC clients (the servers too, with the `server` feature).
2. **Adds JSON support** (with the `json` feature) with [`pbjson-build`](https://docs.rs/pbjson-build):
   `serde::Serialize` / `Deserialize` following the
   [proto3 JSON mapping](https://protobuf.dev/programming-guides/json/), so the JSON matches
   the one of the other protobuf implementations.
3. **Makes the records of a stream easy to extract.** The frames of the admin streams are a
   `Data` enum with one variant per record type (`Data::TalsItem(TapeLsItem)`, ...). The build
   script asks prost to derive [`derive_more::TryInto`](https://docs.rs/derive_more) on this
   enum, which gives every record a `TryFrom<Data>`.
