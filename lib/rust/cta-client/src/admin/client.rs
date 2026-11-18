// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! [`AdminClient`]: sends the typed admin commands to the two admin services of the frontend.

use std::pin::Pin;

use cta_grpc_common::{AuthorizationInterceptor, EndpointConfig};
use cta_model::ConversionError;
use cta_protobuf::cta::xrd::{
    Response, cta_rpc_client::CtaRpcClient, cta_rpc_stream_client::CtaRpcStreamClient,
};
#[cfg(feature = "json")]
use serde_json::Value;
use tokio_stream::{Stream, StreamExt};
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use super::{
    AdminError,
    commands::{StreamingAdminCmd, UnaryAdminCmd},
    stream::{check_header, check_response_type, records, request},
};

type Service = InterceptedService<Channel, AuthorizationInterceptor>;

/// The items of a streaming admin command.
///
/// The stream ends after the first error.
pub type AdminStream<T> = Pin<Box<dyn Stream<Item = Result<T, AdminError>> + Send>>;

/// An authenticated client for the admin services of the CTA frontend.
///
/// Handles both unary and streaming commands. The sub-clients share the same channel.
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
    /// # Errors
    ///
    /// Returns the [`AdminError`] matching the gRPC status, or [`AdminError::InvalidResponse`] if the frontend
    /// answered with a non-success response type.
    pub async fn unary<C: UnaryAdminCmd>(&self, cmd: C) -> Result<Response, AdminError> {
        let response = self
            .unary
            .clone()
            .admin(request(cmd.into()))
            .await?
            .into_inner();
        check_response_type(response.r#type(), &response.message_txt)?;
        Ok(response)
    }

    /// Same as [`Self::unary`], for the commands whose response carries nothing of interest (adding, changing or
    /// deleting a resource): only tells whether the command succeeded.
    ///
    /// # Errors
    ///
    /// Same as [`Self::unary`].
    pub async fn run<C: UnaryAdminCmd>(&self, cmd: C) -> Result<(), AdminError> {
        self.unary(cmd).await.map(drop)
    }

    /// Sends a streaming command to `CtaRpcStream.GenericAdminStream` and returns its records.
    ///
    /// The stream header is read before returning, so that the errors reported at the start of the command (user
    /// errors, authentication, ...) are returned here rather than as the first item of the stream.
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
    /// (lowerCamelCase keys, 64-bit integers as strings, enums as names, defaults always printed). Requires the
    /// `json` feature.
    ///
    /// # Errors
    ///
    /// See [`Self::stream`]; a record that cannot be serialized is an [`AdminError::InvalidResponse`] item.
    #[cfg(feature = "json")]
    pub async fn stream_json<C>(&self, cmd: C) -> Result<AdminStream<Value>, AdminError>
    where
        C: StreamingAdminCmd,
        C::Item: serde::Serialize,
    {
        let items = self.stream(cmd).await?;
        Ok(Box::pin(items.map(|item| {
            serde_json::to_value(item?).map_err(|e| {
                AdminError::invalid_response_from("cannot serialize the record as JSON", e)
            })
        })))
    }

    /// Same as [`Self::stream`], but converts each record into the domain type `T` (see [`cta_model`]).
    ///
    /// # Errors
    ///
    /// See [`Self::stream`]; a record that cannot be converted is an [`AdminError::InvalidResponse`] item.
    pub async fn stream_as<C, T>(&self, cmd: C) -> Result<AdminStream<T>, AdminError>
    where
        C: StreamingAdminCmd,
        T: TryFrom<C::Item, Error = ConversionError> + Send + 'static,
    {
        let items = self.stream(cmd).await?;
        Ok(Box::pin(
            items.map(|item| T::try_from(item?).map_err(AdminError::from)),
        ))
    }
}

#[cfg(test)]
mod tests;
