// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Client for the workflow API (`cta-workflow-api`): the events a disk system sends to CTA.
//!
//! The events are defined in [`cta_model::events`]: one struct per event (`Create`, `Archive`, `Retrieve`,
//! `CancelRetrieve`, `Delete`) describing the file (`FileInfo`) and the user (`Requester`). [`WorkflowClient::send`]
//! encodes an event for the disk instance of the client (with [`cta_model::events::Event::notification`], which first
//! checks what the workflow API would reject before processing it), and sends it to the RPC of the event
//! ([`WorkflowEvent`], [`WorkflowRpc`]). Errors are [`WorkflowError`]s.
//!
//! ```no_run
//! use cta_client::workflow::WorkflowClient;
//! use cta_model::{
//!     events::{Archive, FileInfo, Requester},
//!     file::{Checksum, ChecksumType},
//! };
//! use cta_grpc_common::{EndpointConfig, JwtAuth};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = EndpointConfig::new(
//!     "https://cta-workflow-api.example.org:10956".parse()?,
//!     JwtAuth::new(std::fs::read("/etc/cta/token.jwt")?)?,
//!     None,
//!     None,
//! );
//! let client = WorkflowClient::connect(&config, "eosctaatlas").await?;
//!
//! let file = FileInfo::builder()
//!     .archive_file_id(4_242)
//!     .disk_file_id("42")
//!     .path("/eos/atlas/file")
//!     .size(1_024)
//!     .checksums(vec![Checksum::new(ChecksumType::Adler32, 0x0a0b_0c0d_u32.to_le_bytes().to_vec())])
//!     .build();
//! let event = Archive::builder()
//!     .file(file)
//!     .storage_class("atlas")
//!     .requester(Requester::new("atlas001", "zp"))
//!     .service_url("root://eosatlas.example.org//eos/atlas/file?eos.lfn=fxid:0000002a")
//!     .build();
//! let reply = client.send(&event).await?;
//! println!("queued as {}", reply.response.request_objectstore_id);
//! # Ok(())
//! # }
//! ```
//!
//! The request can also be built first ([`WorkflowClient::request`]), to be kept (e.g. displayed) whatever the outcome
//! of the call, then sent with [`WorkflowClient::submit`].

mod client;
mod error;
mod request;

pub use client::{DEFAULT_TIMEOUT, WorkflowClient, WorkflowReply};
pub use error::WorkflowError;
pub use request::{WorkflowEvent, WorkflowRequest, WorkflowRpc};
