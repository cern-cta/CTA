// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Client for the CTA frontend's admin gRPC interface.
//!
//! The frontend exposes admin commands through two services: a unary one
//! (`CtaRpc`, one aggregated [`Response`]) and a streaming one
//! (`CtaRpcStream`, a sequence of [`StreamResponse`] frames). Both are wrapped
//! by [`CtaGrpcClient`]
//!
//! ```no_run
//! # use cta_lib::{cta::CtaGrpcClient, rpc::EndpointConfig};
//! # async fn example(config: EndpointConfig) -> Result<(), Box<dyn std::error::Error>> {
//! let unary = CtaGrpcClient::new_unary(&config).await?;
//! let streaming = CtaGrpcClient::new_streaming(&config).await?;
//! # Ok(())
//! # }
//! ```

use cta_protobuf::cta::{
    admin::AdminCmd,
    xrd::{
        Request, Response, StreamResponse, cta_rpc_client::CtaRpcClient,
        cta_rpc_stream_client::CtaRpcStreamClient, request::Request as RequestType,
    },
};
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use crate::rpc::{AuthorizationInterceptor, EndpointConfig, Error};

/// An authenticated client for the CTA frontend.
/// Construct with [`CtaGrpcClient::new_unary`] or [`CtaGrpcClient::new_streaming`].
pub struct CtaGrpcClient<C> {
    _inner: C,
}

/// The generated unary client
pub type UnaryClientType = CtaRpcClient<InterceptedService<Channel, AuthorizationInterceptor>>;
/// The generated streaming client
pub type StreamingClientType =
    CtaRpcStreamClient<InterceptedService<Channel, AuthorizationInterceptor>>;

impl CtaGrpcClient<UnaryClientType> {
    /// Connects to the unary admin service described by `config`.
    ///
    /// # Errors
    ///
    /// Propagates the connection errors of [`EndpointConfig::build_channel`]
    /// and the token errors of [`AuthorizationInterceptor::new`].
    pub async fn new_unary(config: &EndpointConfig) -> Result<Self, Error> {
        let channel = config.build_channel().await?;
        let client = CtaRpcClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone())?,
        ));
        Ok(Self { _inner: client })
    }

    /// Sends an admin command and returns the single aggregated response.
    ///
    /// Note that a successful return value only means the call itself
    /// succeeded: callers must still inspect `Response::type` to find out
    /// whether CTA accepted the command.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Status`] if the RPC fails.
    pub async fn admin_cmd(&mut self, cmd: AdminCmd) -> Result<tonic::Response<Response>, Error> {
        self._inner
            .admin(Request {
                request: Some(RequestType::Admincmd(Box::new(cmd))),
            })
            .await
            .map_err(Error::Status)
    }
}

impl CtaGrpcClient<StreamingClientType> {
    /// Connects to the streaming admin service described by `config`.
    ///
    /// # Errors
    ///
    /// Propagates the connection errors of [`EndpointConfig::build_channel`]
    /// and the token errors of [`AuthorizationInterceptor::new`].
    pub async fn new_streaming(config: &EndpointConfig) -> Result<Self, Error> {
        let channel = config.build_channel().await?;
        let client = CtaRpcStreamClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone())?,
        ));
        Ok(Self { _inner: client })
    }

    /// Sends an admin command and returns the raw response stream.
    ///
    /// Prefer consuming the result through
    /// [`StreamResponseExt::stream_response`](crate::StreamResponseExt::stream_response),
    /// which validates the stream header and yields the payload items directly.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Status`] if the RPC fails.
    pub async fn admin_cmd(
        &mut self,
        cmd: AdminCmd,
    ) -> Result<tonic::Streaming<StreamResponse>, Error> {
        self._inner
            .generic_admin_stream(Request {
                request: Some(RequestType::Admincmd(Box::new(cmd))),
            })
            .await
            .map_err(Error::Status)
            .map(|r| r.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;
    use crate::rpc::JwtAuth;

    /// An endpoint on the loopback interface where nothing is ever listening.
    fn unreachable_config() -> EndpointConfig {
        EndpointConfig::new(
            Url::parse("http://127.0.0.1:1").expect("test endpoint should parse"),
            JwtAuth::new(b"a.token.value".to_vec()),
            None,
            None,
        )
    }

    #[tokio::test]
    async fn new_unary_fails_on_an_unreachable_frontend() {
        let error = CtaGrpcClient::new_unary(&unreachable_config())
            .await
            .err()
            .expect("connecting to a closed port should fail");

        assert!(
            matches!(error, Error::Transport(_)),
            "expected a Transport error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn new_streaming_fails_on_an_unreachable_frontend() {
        let error = CtaGrpcClient::new_streaming(&unreachable_config())
            .await
            .err()
            .expect("connecting to a closed port should fail");

        assert!(
            matches!(error, Error::Transport(_)),
            "expected a Transport error, got {error:?}"
        );
    }
}
