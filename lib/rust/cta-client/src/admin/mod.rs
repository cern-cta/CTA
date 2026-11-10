// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Generic client for the typed admin commands of [`crate::commands`].
//!
//! [`AdminClient`] sends any [`TypedAdminCmd`] to the right frontend service: the commands answered with a stream
//! (every `ls`, `showqueues`, `version`) go to `CtaRpcStream.GenericAdminStream`, the others to `CtaRpc.Admin`.
//! The records of a stream are returned as the protobuf type of the command ([`StreamingAdminCmd::Item`]), or as
//! the proto3 JSON printed by `cta-admin --jsonl` ([`AdminClient::stream_json`]); errors are [`AdminError`]s.
//!
//! ```no_run
//! use cta_client::{admin::AdminClient, commands::tape_pool};
//! use cta_grpc_common::{EndpointConfig, JwtAuth};
//! use tokio_stream::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = EndpointConfig::new(
//!     "https://cta-admin-api.example.org:17017".parse()?,
//!     JwtAuth::new(std::fs::read("/etc/cta/token.jwt")?)?,
//!     None,
//!     None,
//! );
//! let client = AdminClient::connect(&config).await?;
//!
//! let mut pools = client.stream(tape_pool::Ls::default()).await?;
//! while let Some(pool) = pools.next().await {
//!     let pool = pool?; // a `TapePoolLsItem`
//!     println!("{}: {} tapes", pool.name, pool.num_tapes);
//! }
//!
//! let rm = tape_pool::Rm::builder().name("my_pool").build();
//! client.unary(rm).await?;
//! # Ok(())
//! # }
//! ```

use std::pin::Pin;

use cta_grpc_common::{AuthorizationInterceptor, EndpointConfig};
use cta_protobuf::cta::{
    admin::AdminCmd,
    xrd::{
        Request, Response, StreamResponse, cta_rpc_client::CtaRpcClient,
        cta_rpc_stream_client::CtaRpcStreamClient, data::Data, request::Request as RequestType,
        response::ResponseType, stream_response::Contents,
    },
};
use serde_json::Value;
use tokio_stream::{Stream, StreamExt};
use tonic::{Streaming, service::interceptor::InterceptedService, transport::Channel};

use crate::{
    commands::{StreamingAdminCmd, TypedAdminCmd},
    errors::AdminError,
};

type Service = InterceptedService<Channel, AuthorizationInterceptor>;

/// The items of a streaming admin command.
///
/// The stream ends after the first error.
pub type AdminStream<T> = Pin<Box<dyn Stream<Item = Result<T, AdminError>> + Send>>;

/// An authenticated client for the admin services of the CTA frontend.
///
/// Both services share one channel. The client is cheap to clone, and its methods take `&self`, so it can be used by
/// concurrent tasks.
#[derive(Clone)]
pub struct AdminClient {
    unary: CtaRpcClient<Service>,
    streaming: CtaRpcStreamClient<Service>,
}

impl AdminClient {
    /// Connects to the admin endpoint described by `config`.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Transport`] if the channel cannot be configured or established, or if the token cannot
    /// be sent as a header.
    pub async fn connect(config: &EndpointConfig) -> Result<Self, AdminError> {
        let channel = config.build_channel().await?;
        let interceptor = AuthorizationInterceptor::new(config.authentication.clone())?;
        Ok(Self {
            unary: CtaRpcClient::new(InterceptedService::new(
                channel.clone(),
                interceptor.clone(),
            )),
            streaming: CtaRpcStreamClient::new(InterceptedService::new(channel, interceptor)),
        })
    }

    /// Sends a unary command to `CtaRpc.Admin` and returns the response.
    ///
    /// Only compiles for commands that are not [`TypedAdminCmd::STREAMING`]:
    ///
    /// ```compile_fail
    /// # use cta_client::{admin::AdminClient, commands::tape_pool};
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// # let client = None::<AdminClient>.unwrap();
    /// client.unary(tape_pool::Ls::default()).await;
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the [`AdminError`] matching the gRPC status, or [`AdminError::InvalidResponse`] if the frontend
    /// answered with a non-success response type.
    pub async fn unary<C: TypedAdminCmd>(&self, cmd: C) -> Result<Response, AdminError> {
        const {
            assert!(
                !C::STREAMING,
                "streaming commands must be sent with `AdminClient::stream`"
            )
        };
        let response = self
            .unary
            .clone()
            .admin(request(cmd.into()))
            .await?
            .into_inner();
        check_response_type(response.r#type(), &response.message_txt)?;
        Ok(response)
    }

    /// Sends a streaming command to `CtaRpcStream.GenericAdminStream` and returns its records.
    ///
    /// The stream header is read before returning, so that the errors reported at the start of the command (user
    /// errors, authentication, ...) are returned here rather than as the first item of the stream.
    ///
    /// Only compiles for [`StreamingAdminCmd`]s:
    ///
    /// ```compile_fail
    /// # use cta_client::{admin::AdminClient, commands::tape_pool};
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// # let client = None::<AdminClient>.unwrap();
    /// client.stream(tape_pool::Rm::builder().name("pool").build()).await;
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the [`AdminError`] matching the gRPC status or the stream header, or
    /// [`AdminError::InvalidResponse`] if the stream does not start with a header. The items of the stream are
    /// [`AdminError::InvalidResponse`] for a record of another type than [`StreamingAdminCmd::Item`].
    pub async fn stream<C: StreamingAdminCmd>(
        &self,
        cmd: C,
    ) -> Result<AdminStream<C::Item>, AdminError> {
        let mut stream = self
            .streaming
            .clone()
            .generic_admin_stream(request(cmd.into()))
            .await?
            .into_inner();
        check_header(&mut stream).await?;
        Ok(Box::pin(records(stream, C::item)))
    }

    /// Same as [`Self::stream`], but returns each record as the proto3 JSON printed by `cta-admin --jsonl`
    /// (lowerCamelCase keys, 64-bit integers as strings, enums as names, defaults always printed).
    ///
    /// # Errors
    ///
    /// See [`Self::stream`]; a record that cannot be serialized is an [`AdminError::InvalidResponse`] item.
    pub async fn stream_json<C: StreamingAdminCmd>(
        &self,
        cmd: C,
    ) -> Result<AdminStream<Value>, AdminError> {
        let items = self.stream(cmd).await?;
        Ok(Box::pin(items.map(|item| {
            serde_json::to_value(item?).map_err(|e| {
                AdminError::invalid_response_from("cannot serialize the record as JSON", e)
            })
        })))
    }
}

fn request(cmd: AdminCmd) -> Request {
    Request {
        request: Some(RequestType::Admincmd(Box::new(cmd))),
    }
}

/// Checks the type of a unary response or stream header.
///
/// The frontend reports every error as the status of the call, so anything other than `RSP_SUCCESS` breaks the
/// protocol.
fn check_response_type(r#type: ResponseType, message: &str) -> Result<(), AdminError> {
    match r#type {
        ResponseType::RspSuccess => Ok(()),
        other => Err(AdminError::invalid_response(format!(
            "unexpected response type {}: {message}",
            other.as_str_name()
        ))),
    }
}

/// Reads the first frame of `stream`, which must be a success header.
async fn check_header(stream: &mut Streaming<StreamResponse>) -> Result<(), AdminError> {
    match stream.message().await? {
        Some(StreamResponse {
            contents: Some(Contents::Header(header)),
        }) => check_response_type(header.r#type(), &header.message_txt),
        Some(StreamResponse {
            contents: Some(Contents::Data(_)),
        }) => Err(AdminError::invalid_response(
            "stream data received before the header",
        )),
        Some(StreamResponse { contents: None }) => Err(AdminError::invalid_response(
            "empty frame received before the header",
        )),
        None => Err(AdminError::invalid_response(
            "stream ended without a header",
        )),
    }
}

/// The records of `stream` (after the header), extracted with `item` (the [`StreamingAdminCmd::item`] of the
/// command), ending after the first error.
fn records<T: 'static>(
    stream: Streaming<StreamResponse>,
    item: fn(Data) -> Result<T, Box<Data>>,
) -> impl Stream<Item = Result<T, AdminError>> + Send {
    let mut failed = false;
    stream
        .filter_map(move |frame| match frame {
            Ok(StreamResponse {
                contents: Some(Contents::Data(data)),
            }) => data.data.map(|data| {
                item(data).map_err(|other| {
                    AdminError::invalid_response(format!("unexpected record: {other:?}"))
                })
            }),
            Ok(StreamResponse {
                contents: Some(Contents::Header(_)),
            }) => Some(Err(AdminError::invalid_response(
                "second stream header received",
            ))),
            // frames without a payload carry nothing to report
            Ok(StreamResponse { contents: None }) => None,
            Err(status) => Some(Err(status.into())),
        })
        // `Streaming` already ends after a failed status; this also ends the stream after a protocol error
        .map_while(move |item| {
            if failed {
                return None;
            }
            failed = item.is_err();
            Some(item)
        })
}

#[cfg(test)]
mod tests;
