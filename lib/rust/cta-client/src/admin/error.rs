// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Errors of the admin commands sent through [`AdminClient`](super::AdminClient).

use std::{borrow::Cow, path::PathBuf};

use cta_grpc_common::Error as RpcError;
use cta_model::ConversionError;

use super::dao::IdFileError;

/// The underlying error of an [`AdminError::InvalidResponse`].
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An error returned by an admin command sent through [`crate::admin::AdminClient`].
///
/// The variants follow the way the frontend API reports errors, so that callers can tell user errors (bad arguments,
/// unknown or already existing resources) from failures of the frontend API or of the connection. Each variant carries, as
/// its [`source`](std::error::Error::source), the gRPC status or error it comes from.
///
/// The data access objects of [`crate::admin::dao`] add the client-side errors: a `get` matching no record
/// ([`AdminError::NotFound`]) or several ([`AdminError::MoreThanOne`]), and an unreadable list of identifiers
/// ([`AdminError::IdFile`]).
#[derive(thiserror::Error, Debug)]
pub enum AdminError {
    /// The frontend API rejected the command as a user error
    #[error("User error: {}", .0.message())]
    User(#[source] tonic::Status),
    /// The command is disabled on this frontend API (e.g. repack commands on an `admin_mode = no_repack` admin API)
    #[error("Command disabled: {}", .0.message())]
    CommandDisabled(#[source] tonic::Status),
    /// The frontend API failed to process the command
    #[error("API error: {}", .0.message())]
    API(#[source] tonic::Status),
    /// The frontend API did not accept the credentials
    #[error("Unauthenticated: {}", .0.message())]
    Unauthenticated(#[source] tonic::Status),
    /// The call was cancelled
    #[error("Cancelled: {}", .0.message())]
    Cancelled(#[source] tonic::Status),
    /// The frontend API could not be reached, or the connection could not be configured or was lost
    #[error(transparent)]
    Transport(RpcError),
    /// The frontend API answered with something that does not follow the protocol, or with a record that cannot be
    /// converted into its domain type
    #[error("Invalid response: {message}")]
    InvalidResponse {
        /// What is wrong with the response
        message: String,
        /// The error raised while decoding or converting the response, if any
        #[source]
        source: Option<BoxedError>,
    },
    /// No resource matches the identifier given to a `get`
    #[error("{resource} {id} not found")]
    NotFound {
        /// The kind of resource (e.g. `tape pool`)
        resource: &'static str,
        /// The identifier, as given
        id: String,
    },
    /// Several resources match the identifier given to a `get`
    #[error("{resource} {id} has {count} matches, expected one")]
    MoreThanOne {
        /// The kind of resource (e.g. `tape file`)
        resource: &'static str,
        /// The identifier, as given
        id: String,
        /// The number of matching resources
        count: usize,
    },
    /// A list of identifiers cannot be read from a file
    #[error("cannot read the identifiers in {}: {source}", path.display())]
    IdFile {
        /// The file
        path: PathBuf,
        /// What went wrong
        #[source]
        source: IdFileError,
    },
}

impl AdminError {
    /// The message of the error, without its category nor its source.
    #[must_use]
    pub fn message(&self) -> Cow<'_, str> {
        match self {
            Self::User(status)
            | Self::API(status)
            | Self::CommandDisabled(status)
            | Self::Unauthenticated(status)
            | Self::Cancelled(status)
            | Self::Transport(RpcError::Status(status)) => status.message().into(),
            Self::Transport(error) => error.to_string().into(),
            Self::InvalidResponse { message, .. } => message.into(),
            Self::NotFound { .. } | Self::MoreThanOne { .. } | Self::IdFile { .. } => {
                self.to_string().into()
            }
        }
    }

    /// The gRPC status the error comes from, if it was reported as the status of the call (by the frontend API or by the
    /// client side of the connection).
    #[must_use]
    pub fn status(&self) -> Option<&tonic::Status> {
        match self {
            Self::User(status)
            | Self::API(status)
            | Self::CommandDisabled(status)
            | Self::Unauthenticated(status)
            | Self::Cancelled(status)
            | Self::Transport(RpcError::Status(status)) => Some(status),
            Self::Transport(_)
            | Self::InvalidResponse { .. }
            | Self::NotFound { .. }
            | Self::MoreThanOne { .. }
            | Self::IdFile { .. } => None,
        }
    }

    /// An [`AdminError::InvalidResponse`] without a source.
    pub(crate) fn invalid_response(message: impl Into<String>) -> Self {
        Self::InvalidResponse {
            message: message.into(),
            source: None,
        }
    }

    /// An [`AdminError::InvalidResponse`] caused by `source`.
    pub(crate) fn invalid_response_from(
        message: impl Into<String>,
        source: impl Into<BoxedError>,
    ) -> Self {
        Self::InvalidResponse {
            message: message.into(),
            source: Some(source.into()),
        }
    }
}

impl From<tonic::Status> for AdminError {
    fn from(status: tonic::Status) -> Self {
        use std::error::Error as _;

        use tonic::Code;

        // Statuses created on the client side from a connection failure keep the failure as their source, the ones
        // sent by the frontend API never have one
        if status.source().is_some() {
            return Self::Transport(RpcError::Status(status));
        }
        match status.code() {
            Code::InvalidArgument => Self::User(status),
            Code::Unimplemented => Self::CommandDisabled(status),
            Code::Unauthenticated => Self::Unauthenticated(status),
            Code::Cancelled => Self::Cancelled(status),
            Code::Unavailable | Code::DeadlineExceeded => Self::Transport(RpcError::Status(status)),
            Code::Internal => Self::invalid_response_from(status.message().to_owned(), status),
            _ => Self::API(status),
        }
    }
}

impl From<RpcError> for AdminError {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::Status(status) => status.into(),
            error => Self::Transport(error),
        }
    }
}

impl From<ConversionError> for AdminError {
    fn from(error: ConversionError) -> Self {
        Self::invalid_response_from(format!("record cannot be converted: {error}"), error)
    }
}
