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
pub(crate) mod test_support;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use cta_protobuf::cta::xrd::{
    StreamResponse, data::Data, response::ResponseType, stream_response::Contents,
};
use eos_protobuf::eos::rpc::MdResponse;
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
/// frames. This adapter validates the header, skips framing artifacts and yields
/// only the payloads ([`Data`]), so callers can treat an admin command like a
/// plain stream of records.
pub struct CtaResponseIter<'t> {
    pub(crate) response: &'t mut Streaming<StreamResponse>,
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
                        if header.r#type() != ResponseType::RspSuccess {
                            return Poll::Ready(Some(Err(ResponseError::CtaStreamError(
                                header.r#type(),
                            ))));
                        }
                        // header ok, not a payload — keep polling for the real data
                    }
                    Contents::Data(data) => {
                        if let Some(d) = data.data {
                            return Poll::Ready(Some(Ok(d)));
                        }
                    }
                },
                Poll::Ready(Some(Ok(StreamResponse { contents: None }))) => {
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(ResponseError::GrpcError(e))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Extension trait that turns a raw gRPC response stream into a higher-level
/// [`Stream`] of individual items.
///
/// It is implemented for the response streams of both services:
///
/// | Stream type | Adapter | Item |
/// | ----------- | ------- | ---- |
/// | `Streaming<StreamResponse>` | [`CtaResponseIter`] | `Result<Data, ResponseError>` |
/// | `Streaming<MdResponse>` | [`EosResponseIter`] | `Result<MdResponse, Status>` |
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
        CtaResponseIter { response: self }
    }
}

/// A [`Stream`] over the [`MdResponse`] items of an EOS metadata query.
pub struct EosResponseIter<'t> {
    pub(crate) response: &'t mut Streaming<MdResponse>,
}

impl<'t> Stream for EosResponseIter<'t> {
    type Item = Result<MdResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut *this.response).poll_next(cx) {
            Poll::Ready(Some(Ok(md_r))) => Poll::Ready(Some(Ok(md_r))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'t> StreamResponseExt<'t, EosResponseIter<'t>> for Streaming<MdResponse> {
    fn stream_response(&'t mut self) -> EosResponseIter<'t>
    where
        EosResponseIter<'t>: 't,
    {
        EosResponseIter { response: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        corrupt_streaming_response, streaming_response, streaming_response_from_bytes,
    };
    use cta_protobuf::cta::{
        admin::RecycleTapeFileLsItem,
        xrd::{Data as XrdData, Response as XrdResponse},
    };
    use tokio_stream::StreamExt;

    /// A `StreamResponse` carrying just a header with the given response type.
    fn header(r#type: ResponseType) -> StreamResponse {
        StreamResponse {
            contents: Some(Contents::Header(Box::new(XrdResponse {
                r#type: r#type.into(),
                ..Default::default()
            }))),
        }
    }

    /// A `StreamResponse` carrying a recycle-bin item with the given VID.
    fn rtfls_item(vid: &str) -> StreamResponse {
        StreamResponse {
            contents: Some(Contents::Data(Box::new(XrdData {
                data: Some(Data::RtflsItem(RecycleTapeFileLsItem {
                    vid: vid.into(),
                    ..Default::default()
                })),
            }))),
        }
    }

    /// A `StreamResponse` whose data frame carries no payload at all.
    fn empty_data() -> StreamResponse {
        StreamResponse {
            contents: Some(Contents::Data(Box::new(XrdData { data: None }))),
        }
    }

    #[tokio::test]
    async fn cta_stream_yields_payloads_after_a_success_header() {
        let mut response = streaming_response(&[
            header(ResponseType::RspSuccess),
            rtfls_item("V01001"),
            rtfls_item("V01002"),
        ]);

        let items: Vec<_> = response
            .stream_response()
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("stream should succeed");

        let vids: Vec<_> = items
            .iter()
            .map(|d| match d {
                Data::RtflsItem(item) => item.vid.clone(),
                other => panic!("unexpected payload: {other:#?}"),
            })
            .collect();

        assert_eq!(vids, ["V01001", "V01002"]);
    }

    #[tokio::test]
    async fn cta_stream_reports_a_failing_header_as_an_error() {
        let mut response =
            streaming_response(&[header(ResponseType::RspErrUser), rtfls_item("V01001")]);

        let first = response
            .stream_response()
            .next()
            .await
            .expect("stream should yield an item");

        match first {
            Err(ResponseError::CtaStreamError(t)) => assert_eq!(t, ResponseType::RspErrUser),
            other => panic!("expected a CtaStreamError, got {other:#?}"),
        }
    }

    #[tokio::test]
    async fn cta_stream_skips_frames_without_a_payload() {
        let mut response = streaming_response(&[
            header(ResponseType::RspSuccess),
            empty_data(),
            rtfls_item("V01001"),
            empty_data(),
        ]);

        let items: Vec<_> = response
            .stream_response()
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("stream should succeed");

        assert_eq!(items.len(), 1, "only the frame with a payload is yielded");
    }

    #[tokio::test]
    async fn cta_stream_of_an_empty_body_terminates_immediately() {
        let mut response = streaming_response::<StreamResponse>(&[]);

        assert!(response.stream_response().next().await.is_none());
    }

    #[tokio::test]
    async fn cta_stream_maps_a_decoding_failure_to_a_grpc_error() {
        let mut response = corrupt_streaming_response::<StreamResponse>();

        let first = response
            .stream_response()
            .next()
            .await
            .expect("stream should yield an item");

        assert!(
            matches!(first, Err(ResponseError::GrpcError(_))),
            "expected a GrpcError, got {first:#?}"
        );
    }

    #[tokio::test]
    async fn cta_stream_stops_at_a_frame_without_contents() {
        // `contents: None` is the end-of-stream marker of the adapter.
        let mut response = streaming_response(&[
            header(ResponseType::RspSuccess),
            StreamResponse { contents: None },
            rtfls_item("V01001"),
        ]);

        let items: Vec<_> = response
            .stream_response()
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("stream should succeed");

        assert!(items.is_empty(), "nothing after the marker is yielded");
    }

    #[tokio::test]
    async fn eos_stream_yields_every_response_in_order() {
        let responses = [
            MdResponse {
                r#type: 0,
                ..Default::default()
            },
            MdResponse {
                r#type: 1,
                ..Default::default()
            },
        ];
        let mut response = streaming_response(&responses);

        let items: Vec<_> = response
            .stream_response()
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("stream should succeed");

        assert_eq!(items, responses);
    }

    #[tokio::test]
    async fn eos_stream_propagates_a_decoding_failure() {
        let mut response = corrupt_streaming_response::<MdResponse>();

        let first: Option<Result<MdResponse, Status>> = response.stream_response().next().await;

        assert!(
            matches!(first, Some(Err(_))),
            "expected a Status error, got {first:#?}"
        );
    }

    #[tokio::test]
    async fn eos_stream_of_an_empty_body_terminates_immediately() {
        let mut response = streaming_response_from_bytes::<MdResponse>(bytes::Bytes::new());

        assert!(response.stream_response().next().await.is_none());
    }
}
