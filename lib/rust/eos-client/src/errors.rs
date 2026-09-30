// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Errors produced by the EOS client library

use std::num::ParseIntError;

use eos_protobuf::eos::rpc::MdId;

/// An Error coming from the EOS API client
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// No endpoint is registered for the requested disk instance in the
    /// [`crate::EosEndpointMap`].
    #[error("Disk instance '{0}' not found")]
    DiskInstanceNotFound(String),
    /// The connection to the EOS endpoint could not be established.
    #[error("gRPC Connection Error: {0}")]
    Rpc(cern_st_grpc::Error),
    /// EOS returned an error status for the call.
    #[error("gRPC Status Error: {0}")]
    Tonic(#[from] tonic::Status),
    /// A numeric identifier could not be parsed from its string form.
    #[error("Error parsing '{0}': {1}")]
    ParseInt(String, ParseIntError),
    /// The disk file id is not usable (EOS reserves the value `0`).
    #[error("Invalid disk file id: {0}")]
    InvalidDiskFileId(u64),
    /// EOS answered successfully but with a payload that violates the
    /// expectations of the caller (missing metadata, too many results, …).
    #[error("Unexpected return value: {0}")]
    UnexpectedReturnValue(String),
    /// No namespace entry exists for the queried identifier.
    #[error("Not found: {0:?}")]
    NotFound(MdId),
    /// Walking up the parent chain reached the namespace root, so there is no
    /// parent container left to create.
    #[error("Root container reached")]
    RootContainerReached,
    /// Invalid file/container path
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}
