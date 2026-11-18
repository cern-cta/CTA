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
//! | [`admin`] | Client for `cta-admin-api` ([`admin::AdminClient`]) and its typed admin commands ([`admin::commands`]) |
//! | [`types`] | Domain types built from the protobuf records ([`types::File`], [`types::Checksum`], [`types::ChecksumType`]) |
//! | [`errors`] | Errors of the conversions into the domain types ([`errors::ChecksumTypeError`], ...) |
//! | [`macros`] | Helper macros, most notably [`admin_cmd!`] for building admin-command payloads |

//!
//! # Example
//!
//! Listing the contents of the CTA tape-file recycle bin:
//!
//! ```no_run
//! use cta_client::admin::{AdminClient, commands::recycle_tape_file};
//! use cta_grpc_common::{EndpointConfig, JwtAuth};
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
//! let client = AdminClient::connect(&config).await?;
//!
//! let mut items = client.stream(recycle_tape_file::Ls::default()).await?;
//! while let Some(item) = items.next().await {
//!     let item = item?; // a `RecycleTapeFileLsItem`
//!     println!("{} {}", item.vid, item.disk_file_path);
//! }
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod admin;
pub mod errors;
pub mod macros;
pub mod types;

#[cfg(test)]
mod tests;
