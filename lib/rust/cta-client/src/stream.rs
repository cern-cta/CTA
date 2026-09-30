// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Streaming helpers

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use cta_protobuf::cta::xrd::{
    StreamResponse, data::Data, response::ResponseType, stream_response::Contents,
};
use tokio_stream::Stream;
use tonic::Streaming;

use crate::errors::Error as CtaError;

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
#[must_use]
pub struct CtaResponseIter<'t> {
    pub(crate) response: &'t mut Streaming<StreamResponse>,
    header_seen: bool,
}

impl<'t> Stream for CtaResponseIter<'t> {
    type Item = Result<Data, CtaError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut *this.response).poll_next(cx) {
                Poll::Ready(Some(Ok(StreamResponse {
                    contents: Some(contents),
                }))) => match contents {
                    Contents::Header(header) => {
                        if this.header_seen {
                            return Poll::Ready(Some(Err(CtaError::UnexpectedResponseType(
                                ResponseType::RspErrUser,
                            ))));
                        }
                        this.header_seen = true;

                        if header.r#type() != ResponseType::RspSuccess {
                            return Poll::Ready(Some(Err(CtaError::UnexpectedResponseType(
                                header.r#type(),
                            ))));
                        }
                        // header ok, not a payload — keep polling for the real data
                    }
                    Contents::Data(data) => {
                        if !this.header_seen {
                            return Poll::Ready(Some(Err(CtaError::UnexpectedResponseType(
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
                        return Poll::Ready(Some(Err(CtaError::UnexpectedResponseType(
                            ResponseType::RspErrUser,
                        ))));
                    }
                    // A frame with no contents after we've seen the header means EOF.
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(CtaError::Rpc(e.into()))));
                }
                Poll::Ready(None) => {
                    if !this.header_seen {
                        return Poll::Ready(Some(Err(CtaError::UnexpectedResponseType(
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
/// | `Streaming<StreamResponse>` | [`CtaResponseIter`] | `Result<Data, crate::errors::Error>` |
#[must_use]
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
