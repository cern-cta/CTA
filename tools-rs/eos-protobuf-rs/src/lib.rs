// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![doc = include_str!("../README.md")]

/// Bindings for the `eos.*` protobuf packages.
#[allow(
    missing_docs,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    reason = "tonic/prost-generated code, not documented by hand"
)]
pub mod eos {
    /// Generated types of the `eos.console` package: the request and reply
    /// messages of the EOS console commands.
    pub mod console {
        tonic::include_proto!("eos.console");
    }
    /// Generated types of the `eos.rpc` package: namespace metadata, insert
    /// operations and the `EosClient` gRPC client.
    pub mod rpc {
        tonic::include_proto!("eos.rpc");
    }
    /// Generated types of the `eos.traffic_shaping` package.
    pub mod traffic_shaping {
        tonic::include_proto!("eos.traffic_shaping");
    }
}
