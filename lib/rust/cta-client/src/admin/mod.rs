// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The admin API (`cta-admin-api`): its client, the typed admin commands it sends ([`commands`]), and the data access
//! objects built on them ([`dao`]).
//!
//! [`AdminClient`] sends any [`TypedAdminCmd`](commands::TypedAdminCmd) to the right frontend service: the commands answered with a stream
//! (every `ls`, `showqueues`, `version`) go to `CtaRpcStream.GenericAdminStream`, the others to `CtaRpc.Admin`.
//! The records of a stream are returned as the protobuf type of the command ([`StreamingAdminCmd::Item`](commands::StreamingAdminCmd::Item)), or as
//! the proto3 JSON printed by `cta-admin --jsonl` (`AdminClient::stream_json`, with the `json` feature); errors are [`AdminError`]s.
//!
//! ```no_run
//! use cta_client::admin::AdminClient;
//! use cta_client::admin::commands::tape_pool;
//! use cta_grpc_common::{EndpointConfig, JwtAuth};
//! use tokio_stream::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = EndpointConfig::new(
//!     "https://cta-admin-api.example.org:17017".parse()?,
//!     JwtAuth::new(std::fs::read("/etc/cta/token.jwt")?)?,
//!     None,
//!     None,
//! );
//! let client = AdminClient::connect(&config).await?;
//!
//! let mut pools = client.stream(tape_pool::Ls::default()).await?;
//! while let Some(pool) = pools.next().await {
//!     let pool = pool?; // a `TapePoolLsItem`
//!     println!("{}: {} tapes", pool.name, pool.num_tapes);
//! }
//!
//! let rm = tape_pool::Rm::builder().name("my_pool").build();
//! client.unary(rm).await?;
//! # Ok(())
//! # }
//! ```

mod client;
pub mod commands;
pub mod dao;
mod error;
mod stream;
#[cfg(test)]
mod test_support;

pub use client::{AdminClient, AdminStream};
pub use error::{AdminError, BoxedError};
