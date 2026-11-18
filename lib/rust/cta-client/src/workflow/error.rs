// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Errors of the workflow events sent through [`WorkflowClient`](super::WorkflowClient).

use std::borrow::Cow;

use cta_grpc_common::Error as RpcError;
use cta_model::events::InvalidEvent;

/// An error returned by a workflow event sent through [`crate::workflow::WorkflowClient`].
///
/// The variants follow the way the workflow API reports errors: a user error while processing
/// the event is `ABORTED`, a request rejected before processing (or undecodable) is `INVALID_ARGUMENT`, other failures
/// are `FAILED_PRECONDITION`.
#[derive(thiserror::Error, Debug)]
pub enum WorkflowError {
    /// The workflow API rejected the event while processing it (e.g. unknown storage class, no route to tape)
    #[error("User error: {}", .0.message())]
    User(#[source] tonic::Status),
    /// The workflow API rejected the request before processing it (wrong event type, missing storage class or
    /// archive file ID, undecodable request)
    #[error("Invalid request: {}", .0.message())]
    InvalidRequest(#[source] tonic::Status),
    /// The workflow API failed to process the event
    #[error("API error: {}", .0.message())]
    API(#[source] tonic::Status),
    /// The workflow API did not accept the credentials
    #[error("Unauthenticated: {}", .0.message())]
    Unauthenticated(#[source] tonic::Status),
    /// The workflow API could not be reached, the connection could not be configured or was lost, or the call timed
    /// out
    #[error(transparent)]
    Transport(RpcError),
    /// The workflow API answered with something that does not follow the protocol
    #[error("Invalid response: {message}")]
    InvalidResponse {
        /// What is wrong with the response
        message: String,
        /// The error raised while decoding the response, if any
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
    /// The event was not sent, as the workflow API would reject it
    #[error("Invalid event: {0}")]
    InvalidEvent(#[from] InvalidEvent),
}

impl WorkflowError {
    /// The message of the error, without its category nor its source.
    #[must_use]
    pub fn message(&self) -> Cow<'_, str> {
        match self {
            Self::User(status)
            | Self::InvalidRequest(status)
            | Self::API(status)
            | Self::Unauthenticated(status)
            | Self::Transport(RpcError::Status(status)) => status.message().into(),
            Self::Transport(error) => error.to_string().into(),
            Self::InvalidResponse { message, .. } => message.into(),
            Self::InvalidEvent(error) => error.to_string().into(),
        }
    }

    /// The gRPC status the error comes from, if it was reported as the status of the call (by the workflow API or by
    /// the client side of the connection).
    #[must_use]
    pub fn status(&self) -> Option<&tonic::Status> {
        match self {
            Self::User(status)
            | Self::InvalidRequest(status)
            | Self::API(status)
            | Self::Unauthenticated(status)
            | Self::Transport(RpcError::Status(status)) => Some(status),
            Self::Transport(_) | Self::InvalidResponse { .. } | Self::InvalidEvent(_) => None,
        }
    }
}

impl From<tonic::Status> for WorkflowError {
    fn from(status: tonic::Status) -> Self {
        use std::error::Error as _;

        use tonic::Code;

        // Statuses created on the client side from a connection failure keep the failure as their source, the ones
        // sent by the workflow API never have one
        if status.source().is_some() {
            return Self::Transport(RpcError::Status(status));
        }
        match status.code() {
            Code::Aborted => Self::User(status),
            Code::InvalidArgument => Self::InvalidRequest(status),
            Code::Unauthenticated => Self::Unauthenticated(status),
            Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
                Self::Transport(RpcError::Status(status))
            }
            Code::Internal => Self::InvalidResponse {
                message: status.message().to_owned(),
                source: Some(status.into()),
            },
            _ => Self::API(status),
        }
    }
}

impl From<RpcError> for WorkflowError {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::Status(status) => status.into(),
            error => Self::Transport(error),
        }
    }
}
