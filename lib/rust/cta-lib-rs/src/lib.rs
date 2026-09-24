// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! High-level client library for the CTA and EOS gRPC interfaces.
//!
//! This crate sits on top of the generated protobuf/gRPC bindings in
//! [`cta_protobuf`] and [`eos_protobuf`] and provides a thin layer to
//! tools using it.
//!
//! * [`rpc`] — endpoint configuration ([`rpc::EndpointConfig`]), TLS channel
//!   construction and JWT authentication
//! * [`cta`] — client for the CTA frontend ([`cta::CtaGrpcClient`]), available
//!   in a unary and a streaming flavour.
//! * [`eos`] — a client for the EOS namespace gRPC API
//!   ([`eos::EosGrpcClient`]) plus a per-disk-instance endpoint registry
//!   ([`eos::EosEndpointMap`]).
//!
//! # Example
//!
//! Listing the contents of the CTA tape-file recycle bin:
//!
//! ```no_run
//! use cta_lib::{
//!     StreamResponseExt,
//!     cta::CtaGrpcClient,
//!     rpc::{EndpointConfig, JwtAuth},
//! };
//! use cta_protobuf::cta::admin::{AdminCmd, admin_cmd};
//! use tokio_stream::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = EndpointConfig::new(
//!     "https://cta-frontend.example.org:50051".parse()?,
//!     JwtAuth::new(std::fs::read("/etc/cta/token.jwt")?),
//!     None,
//!     None,
//! );
//!
//! let mut client = CtaGrpcClient::new_streaming(&config).await?;
//!
//! let mut cmd = AdminCmd::default();
//! cmd.set_cmd(admin_cmd::Cmd::Recycletapefile);
//! cmd.set_subcmd(admin_cmd::SubCmd::SubcmdLs);
//!
//! let mut response_stream = client.admin_cmd(cmd).await?;
//! let mut items = response_stream.stream_response();
//! while let Some(item) = items.next().await {
//!     println!("{:#?}", item?);
//! }
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod cta;
pub mod eos;
pub mod rpc;

#[cfg(test)]
mod tests;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use cta_protobuf::cta::xrd::{
    StreamResponse, data::Data, response::ResponseType, stream_response::Contents,
};
use tokio_stream::Stream;
use tonic::{Status, Streaming};

/// An error observed while consuming a CTA response stream.
#[derive(Debug, thiserror::Error)]
pub enum ResponseError {
    /// The underlying gRPC call or stream failed.
    #[error("gRPC Error: {0:#?}")]
    GrpcError(Status),
    /// The CTA frontend answered with a stream header that reports a failure,
    /// i.e. anything other than [`ResponseType::RspSuccess`].
    #[error("CTA Stream Error: {0:#?}")]
    CtaStreamError(ResponseType),
}

/// An iter which goes over a stream of response contents and produces the individual results.
///
/// The CTA frontend sends a header frame followed by an arbitrary number of data
/// frames. This adapter enforces that the header arrives first, validates its
/// success/failure status, skips framing artifacts and yields only the payloads
/// ([`Data`]), so callers can treat an admin command like a plain stream of records.
///
/// # Protocol Invariants
///
/// A valid stream is always `[Header, Data*, Data*, ...]` where:
/// - The header *must* be the first frame and *must* indicate success
/// - Any data frames *must* follow the header
/// - A second header is a protocol violation
/// - A stream that ends without a header is an error
pub struct CtaResponseIter<'t> {
    pub(crate) response: &'t mut Streaming<StreamResponse>,
    header_seen: bool,
}

impl<'t> Stream for CtaResponseIter<'t> {
    type Item = Result<Data, ResponseError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut *this.response).poll_next(cx) {
                Poll::Ready(Some(Ok(StreamResponse {
                    contents: Some(contents),
                }))) => match contents {
                    Contents::Header(header) => {
                        if this.header_seen {
                            return Poll::Ready(Some(Err(ResponseError::CtaStreamError(
                                ResponseType::RspErrUser,
                            ))));
                        }
                        this.header_seen = true;

                        if header.r#type() != ResponseType::RspSuccess {
                            return Poll::Ready(Some(Err(ResponseError::CtaStreamError(
                                header.r#type(),
                            ))));
                        }
                        // header ok, not a payload — keep polling for the real data
                    }
                    Contents::Data(data) => {
                        if !this.header_seen {
                            return Poll::Ready(Some(Err(ResponseError::CtaStreamError(
                                ResponseType::RspErrUser,
                            ))));
                        }

                        if let Some(d) = data.data {
                            return Poll::Ready(Some(Ok(d)));
                        }
                        // Skip empty data frames, keep polling
                    }
                },
                Poll::Ready(Some(Ok(StreamResponse { contents: None }))) => {
                    if !this.header_seen {
                        return Poll::Ready(Some(Err(ResponseError::CtaStreamError(
                            ResponseType::RspErrUser,
                        ))));
                    }
                    // A frame with no contents after we've seen the header means EOF.
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(ResponseError::GrpcError(e))));
                }
                Poll::Ready(None) => {
                    if !this.header_seen {
                        return Poll::Ready(Some(Err(ResponseError::CtaStreamError(
                            ResponseType::RspErrUser,
                        ))));
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Extension trait that turns a raw gRPC response stream into a higher-level
/// [`Stream`] of individual items.
///
/// It is implemented for CTA response streams:
///
/// | Stream type | Adapter | Item |
/// | ----------- | ------- | ---- |
/// | `Streaming<StreamResponse>` | [`CtaResponseIter`] | `Result<Data, ResponseError>` |
pub trait StreamResponseExt<'t, T> {
    /// Borrows the stream and wraps it in the matching adapter.
    fn stream_response(&'t mut self) -> T
    where
        T: 't;
}

impl<'t> StreamResponseExt<'t, CtaResponseIter<'t>> for Streaming<StreamResponse> {
    fn stream_response(&'t mut self) -> CtaResponseIter<'t>
    where
        CtaResponseIter<'t>: 't,
    {
        CtaResponseIter {
            response: self,
            header_seen: false,
        }
    }
}
