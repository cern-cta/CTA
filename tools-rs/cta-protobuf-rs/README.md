<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
-->

# cta-protobuf

Generated Rust protobuf/gRPC bindings for the [CTA](https://gitlab.cern.ch/cta/CTA)
frontend interface.

Only minimal scaffolding is written by hand: `build.rs` runs
[`tonic-prost-build`](https://docs.rs/tonic-prost-build) over the `.proto` files
of the `xrootd-ssi-protobuf-interface` submodule
(`lib/protobuf/external/` in the CTA repository), so the bindings always match
the C++ frontend of the same checkout. Initialise the submodules before
building:

```bash
git submodule update --init --recursive
```

Prefer the higher-level wrappers in [`cta-lib`](https://gitlab.cern.ch/cta/CTA/-/blob/main/tools-rs/cta-lib-rs/README.md), which
handle channel setup, authentication and response streaming.
