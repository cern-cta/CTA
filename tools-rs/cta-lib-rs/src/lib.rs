// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod cta;
pub mod eos;
pub mod rpc;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use cta_protobuf::cta::xrd::{
    StreamResponse, data::Data, response::ResponseType, stream_response::Contents,
};
use eos_protobuf::eos::rpc::MdResponse;
use tokio_stream::Stream;
use tonic::{Response, Status, Streaming};

#[derive(Debug, thiserror::Error)]
pub enum ResponseError {
    #[error("gRPC Error: {0:#?}")]
    GrpcError(Status),
    #[error("CTA Stream Error: {0:#?}")]
    CtaStreamError(ResponseType),
}

/// An iter which goes over a stream of response contents and produces the individual results.
pub struct CtaResponseIter<'t> {
    pub(crate) response: &'t mut Response<Streaming<StreamResponse>>,
}

impl<'t> Stream for CtaResponseIter<'t> {
    type Item = Result<Data, ResponseError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(this.response.get_mut()).poll_next(cx) {
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

pub trait StreamResponseExt<'t, T> {
    fn stream_response(&'t mut self) -> T
    where
        T: 't;
}

impl<'t> StreamResponseExt<'t, CtaResponseIter<'t>> for Response<Streaming<StreamResponse>> {
    fn stream_response(&'t mut self) -> CtaResponseIter<'t>
    where
        CtaResponseIter<'t>: 't,
    {
        CtaResponseIter { response: self }
    }
}

pub struct EosResponseIter<'t> {
    pub(crate) response: &'t mut Response<Streaming<MdResponse>>,
}

impl<'t> Stream for EosResponseIter<'t> {
    type Item = Result<MdResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(this.response.get_mut()).poll_next(cx) {
            Poll::Ready(Some(Ok(md_r))) => Poll::Ready(Some(Ok(md_r))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'t> StreamResponseExt<'t, EosResponseIter<'t>> for Response<Streaming<MdResponse>> {
    fn stream_response(&'t mut self) -> EosResponseIter<'t>
    where
        EosResponseIter<'t>: 't,
    {
        EosResponseIter { response: self }
    }
}
