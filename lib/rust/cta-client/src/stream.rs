// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Streaming helpers

use std::{
    borrow::BorrowMut,
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
/// A valid stream is always `[Header, Data*, Data*, ...]` where:
/// - The header *must* be the first frame and *must* indicate success
/// - Any data frames *must* follow the header
/// - A second header is a protocol violation
/// - A stream that ends without a header is an error
#[must_use]
pub struct CtaResponseIter<R = Streaming<StreamResponse>> {
    pub(crate) response: R,
    header_seen: bool,
}

impl<R> Stream for CtaResponseIter<R>
where
    R: BorrowMut<Streaming<StreamResponse>> + Unpin,
{
    type Item = Result<Data, CtaError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(this.response.borrow_mut()).poll_next(cx) {
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
                    }
                },
                Poll::Ready(Some(Ok(StreamResponse { contents: None }))) => {
                    if !this.header_seen {
                        return Poll::Ready(Some(Err(CtaError::UnexpectedResponseType(
                            ResponseType::RspErrUser,
                        ))));
                    }
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
#[must_use]
pub trait StreamResponseExt: Sized {
    /// Borrows the stream and wraps it in the adapter.
    fn stream_response(&mut self) -> CtaResponseIter<&mut Self>;

    /// Takes ownership of the stream; the result has no borrow and can be returned.
    fn into_stream_response(self) -> CtaResponseIter<Self>;
}

impl StreamResponseExt for Streaming<StreamResponse> {
    fn stream_response(&mut self) -> CtaResponseIter<&mut Self> {
        CtaResponseIter {
            response: self,
            header_seen: false,
        }
    }

    fn into_stream_response(self) -> CtaResponseIter<Self> {
        CtaResponseIter {
            response: self,
            header_seen: false,
        }
    }
}
