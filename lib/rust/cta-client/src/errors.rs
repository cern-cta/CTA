// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Error types raised by the CTA client

use cern_st_grpc::Error as RpcError;
use cta_protobuf::cta::xrd::{data::Data, response::ResponseType};

/// An error happening at the level of a [`crate::client::CtaGrpcClient`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// A gRPC error, which happens at the lower (rpc) level of this lib
    #[error("gRPC Error: {0}")]
    Rpc(#[from] RpcError),
    /// An unexpected value was returned by a data stream (e.g. two elements instead of one)
    #[error("Stream returned unexpected data: {0:#?}")]
    UnexpectedStreamData(Box<Data>),
    /// The CTA frontend returned a response type which indicates a problem
    #[error("The CTA frontend answered with a non-success response type: {0:#?}")]
    UnexpectedResponseType(ResponseType),
    /// The CTA frontend returned an invalid or incomplete protobuf message.
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
    /// Something was not found by the frontend (specified in string)
    #[error("Not found: {0}")]
    NotFound(String),
}
