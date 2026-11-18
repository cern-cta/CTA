// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! [`WorkflowClient`]: sends the events of a disk instance to the workflow API.

use std::time::Duration;

use cta_grpc_common::{AuthorizationInterceptor, EndpointConfig};
use cta_protobuf::cta::xrd::{Response, cta_rpc_client::CtaRpcClient, response::ResponseType};
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use super::{WorkflowError, WorkflowEvent, WorkflowRequest, WorkflowRpc};

type Service = InterceptedService<Channel, AuthorizationInterceptor>;

/// Timeout of a call, unless set with [`WorkflowClient::with_timeout`]
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// An authenticated client for the workflow API, sending the events of one disk instance.
#[derive(Clone)]
pub struct WorkflowClient {
    rpc: CtaRpcClient<Service>,
    disk_instance: String,
    timeout: Option<Duration>,
}

impl WorkflowClient {
    /// Connects to the workflow endpoint described by `config`, to send the events of `disk_instance`
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError::Transport`] if the channel cannot be configured or established, or if the token cannot
    /// be sent as a header.
    pub async fn connect(
        config: &EndpointConfig,
        disk_instance: impl Into<String>,
    ) -> Result<Self, WorkflowError> {
        let channel = config.build_channel().await?;
        let interceptor = AuthorizationInterceptor::new(config.authentication.clone())?;
        Ok(Self {
            rpc: CtaRpcClient::new(InterceptedService::new(channel, interceptor)),
            disk_instance: disk_instance.into(),
            timeout: Some(DEFAULT_TIMEOUT),
        })
    }

    /// The same client, with `timeout` as the timeout of each call (`None`: no timeout).
    #[must_use]
    pub fn with_timeout(self, timeout: Option<Duration>) -> Self {
        Self { timeout, ..self }
    }

    /// The name of the disk instance sending the events.
    #[must_use]
    pub fn disk_instance(&self) -> &str {
        &self.disk_instance
    }

    /// Builds the request of `event`, without sending it.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError::InvalidEvent`] if the workflow API would reject the event before processing it.
    pub fn request<E: WorkflowEvent>(&self, event: &E) -> Result<WorkflowRequest, WorkflowError> {
        Ok(WorkflowRequest::new(event, &self.disk_instance)?)
    }

    /// Sends `event` to its RPC.
    ///
    /// # Errors
    ///
    /// See [`Self::request`] and [`Self::submit`].
    pub async fn send<E: WorkflowEvent>(&self, event: &E) -> Result<WorkflowReply, WorkflowError> {
        self.submit(self.request(event)?).await
    }

    /// Sends a request built by [`Self::request`] to its RPC.
    ///
    /// # Errors
    ///
    /// Returns the [`WorkflowError`] matching the gRPC status, or [`WorkflowError::InvalidResponse`] if the workflow API
    /// answered with a non-success response type.
    pub async fn submit(&self, request: WorkflowRequest) -> Result<WorkflowReply, WorkflowError> {
        let mut call = tonic::Request::new(request.request());
        if let Some(timeout) = self.timeout {
            call.set_timeout(timeout);
        }
        let mut rpc = self.rpc.clone();

        // build the request for the RPC of the event, and send it to the workflow API
        let response = match request.rpc() {
            WorkflowRpc::Create => rpc.create(call).await,
            WorkflowRpc::Archive => rpc.archive(call).await,
            WorkflowRpc::Retrieve => rpc.retrieve(call).await,
            WorkflowRpc::CancelRetrieve => rpc.cancel_retrieve(call).await,
            WorkflowRpc::Delete => rpc.delete(call).await,
        }?
        .into_inner();

        // the workflow API reports errors as a non-success response type
        if response.r#type() != ResponseType::RspSuccess {
            return Err(WorkflowError::InvalidResponse {
                message: format!(
                    "unexpected response type {}: {}",
                    response.r#type().as_str_name(),
                    response.message_txt
                ),
                source: None,
            });
        }
        Ok(WorkflowReply { request, response })
    }
}

/// The successful outcome of an event.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowReply {
    /// The request sent
    pub request: WorkflowRequest,
    /// The response of the workflow API: `RSP_SUCCESS`, with the attributes the disk system should set on the file
    /// (`xattr`, and some of them as fields: `archive_file_id` for a `Create`, `request_objectstore_id` for an
    /// `Archive` or a `Retrieve`, ...)
    pub response: Response,
}

impl WorkflowReply {
    /// The message of the response (`Response.message_txt`).
    #[must_use]
    pub fn message(&self) -> &str {
        &self.response.message_txt
    }

    /// The response, as proto3 JSON (lowerCamelCase keys, 64-bit integers as strings, enums as names). Requires the
    /// `json` feature.
    ///
    /// # Errors
    ///
    /// Returns the serialization error, which cannot happen for a decoded response.
    #[cfg(feature = "json")]
    pub fn response_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(&self.response)
    }
}

#[cfg(test)]
mod tests;
