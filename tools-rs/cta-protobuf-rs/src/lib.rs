// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![doc = include_str!("../README.md")]

/// Bindings for the `cta.*` protobuf packages.
#[allow(
    missing_docs,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    reason = "tonic/prost-generated code, not documented by hand"
)]
pub mod cta {
    /// Generated types of the `cta.admin` package: admin commands, their
    /// options and the records returned by the `cta-admin` queries.
    pub mod admin {
        tonic::include_proto!("cta.admin");
    }
    /// Generated types of the `cta.common` package: value types shared across
    /// the CTA interfaces.
    pub mod common {
        tonic::include_proto!("cta.common");
    }
    /// Generated types of the `cta.eos` package: workflow events and
    /// notifications exchanged between EOS and the CTA frontend.
    pub mod eos {
        tonic::include_proto!("cta.eos");
    }
    /// Generated types of the `cta.xrd` package: the frontend request/response
    /// envelope and the unary and streaming gRPC clients.
    pub mod xrd {
        tonic::include_proto!("cta.xrd");
    }
}
