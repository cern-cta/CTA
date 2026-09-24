<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
-->

# cta-lib

High-level client library for the [CTA](https://gitlab.cern.ch/cta/CTA)
frontend and the EOS namespace gRPC APIs. It wraps the generated bindings of
`cta-protobuf` and `eos-protobuf` and provides a thin layer on top of them.

## Usage

Full API documentation:

```bash
cargo doc -p cta-lib --no-deps --open
```

## Tests

```bash
cargo test -p cta-lib
```
