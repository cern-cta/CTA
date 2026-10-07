// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![forbid(unsafe_code)]

//! High-level client library for the CTA frontend gRPC interface.
//!
//! This crate sits on top of the generated protobuf/gRPC bindings in
//! [`cta_protobuf`] and provides a thin layer to tools using it.
//!
//! | Module | Description |
//! | ------ | ----------- |
//! | [`client`] | CTA frontend client in unary and streaming flavours ([`client::CtaGrpcClient`]) |
//! | [`types`] | Domain types wrapping protobuf representations ([`types::File`], [`types::Checksum`], [`types::ChecksumType`]) |
//! | [`stream`] | Streaming adapters, most notably [`stream::StreamResponseExt`] |
//! | [`errors`] | Error types raised by client operations ([`errors::Error`]) |
//! | [`macros`] | Helper macros, most notably [`admin_cmd!`] for building admin-command payloads |

//!
//! # Example
//!
//! Listing the contents of the CTA tape-file recycle bin:
//!
//! ```no_run
//! use cta_client::{
//!     admin_cmd,
//!     client::CtaGrpcClient,
//!     stream::StreamResponseExt,
//! };
//! use cta_grpc_common::{EndpointConfig, JwtAuth};
//! use cta_protobuf::cta::admin::AdminCmd;
//! use tokio_stream::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = EndpointConfig::new(
//!     "https://cta-frontend.example.org:50051".parse()?,
//!     JwtAuth::new(std::fs::read("/etc/cta/token.jwt")?)?,
//!     None,
//!     None,
//! );
//!
//! let mut client = CtaGrpcClient::new_streaming(&config).await?;
//!
//! let mut cmd = admin_cmd! (Recycletapefile.SubcmdLs {});
//!
//! let mut response_stream = client.raw_admin_cmd(cmd).await?;
//! let mut items = response_stream.stream_response();
//! while let Some(item) = items.next().await {
//!     println!("{:#?}", item?);
//! }
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod client;
pub mod errors;
pub mod macros;
pub mod stream;
pub mod types;

#[cfg(test)]
mod tests;
