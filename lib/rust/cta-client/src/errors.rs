// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Error types raised by the CTA client

use cta_grpc_common::Error as RpcError;
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

/// An error type for the conversion of checksum types. Useful to convert between EOS and CTA.
#[derive(Debug, thiserror::Error)]
pub enum ChecksumTypeError<T> {
    /// Error parsing a checksum name string
    #[error("Error parsing string '{0}'")]
    Parse(String),
    /// Error converting from a source checksum value
    #[error("Source checksum value not supported: {0:?}")]
    UnknownType(T),
}

/// Error type for failures during archive file conversion from protobuf.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveFileConversionError {
    /// The archive file creation timestamp was out of valid range
    #[error("Invalid archive file creation timestamp: {0}")]
    InvalidTimestamp(i64),
    /// One or more checksums could not be converted
    #[error("Failed to convert checksum: {0}")]
    ChecksumConversion(String),
}

impl From<ArchiveFileConversionError> for Error {
    fn from(e: ArchiveFileConversionError) -> Self {
        Error::InvalidResponse(e.to_string())
    }
}

/// Error type for failures during recycle tape file list item conversion.
#[derive(Debug, thiserror::Error)]
pub enum RecycleTapeFileConversionError {
    /// The archive file creation timestamp was out of valid range
    #[error("Invalid archive file creation timestamp: {0}")]
    InvalidTimestamp(i64),
    /// One or more checksums could not be converted
    #[error("Failed to convert checksum: {0}")]
    ChecksumConversion(String),
}

impl From<RecycleTapeFileConversionError> for Error {
    fn from(e: RecycleTapeFileConversionError) -> Self {
        Error::InvalidResponse(e.to_string())
    }
}

/// Error returned when a [`cta_protobuf::cta::admin::TapeFileLsItem`] cannot be converted to a [`crate::types::File`]
/// because a required protobuf sub-message is absent.
#[derive(Debug, thiserror::Error)]
#[error("TapeFileLsItem missing field: {0}")]
pub struct TapeFileLsItemConversionError(pub String);

impl From<TapeFileLsItemConversionError> for Error {
    fn from(e: TapeFileLsItemConversionError) -> Self {
        Error::InvalidResponse(e.to_string())
    }
}
