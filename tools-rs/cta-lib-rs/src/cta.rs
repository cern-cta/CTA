// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use std::marker::PhantomData;

use cta_protobuf::cta::{
    admin::AdminCmd,
    xrd::{
        Request, Response, StreamResponse, cta_rpc_client::CtaRpcClient,
        cta_rpc_stream_client::CtaRpcStreamClient, request::Request as RequestType,
    },
};
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use crate::rpc::{AuthorizationInterceptor, EndpointConfig, Error};

pub struct Streaming;
pub struct Unary;

pub struct CtaGrpcClient<T, C> {
    _inner: C,
    _t: PhantomData<T>,
}

pub type UnaryClientType = CtaRpcClient<InterceptedService<Channel, AuthorizationInterceptor>>;
pub type StreamingClientType =
    CtaRpcStreamClient<InterceptedService<Channel, AuthorizationInterceptor>>;

impl<T, C> CtaGrpcClient<T, C> {
    pub fn connection(&self) -> &C {
        &self._inner
    }
}

impl CtaGrpcClient<Unary, UnaryClientType> {
    pub async fn new_unary(config: &EndpointConfig) -> Result<Self, Error> {
        let channel = config.build_channel().await?;
        let client = CtaRpcClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone()),
        ));
        Ok(Self {
            _inner: client,
            _t: PhantomData,
        })
    }

    pub async fn admin_cmd(&mut self, cmd: AdminCmd) -> Result<tonic::Response<Response>, Error> {
        self._inner
            .admin(Request {
                request: Some(RequestType::Admincmd(Box::new(cmd))),
            })
            .await
            .map_err(Error::Status)
    }
}

impl CtaGrpcClient<Streaming, StreamingClientType> {
    pub async fn new_streaming(config: &EndpointConfig) -> Result<Self, Error> {
        let channel = config.build_channel().await?;
        let client = CtaRpcStreamClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone()),
        ));
        Ok(Self {
            _inner: client,
            _t: PhantomData,
        })
    }

    pub async fn admin_cmd(
        &mut self,
        cmd: AdminCmd,
    ) -> Result<tonic::Response<tonic::Streaming<StreamResponse>>, Error> {
        self._inner
            .generic_admin_stream(Request {
                request: Some(RequestType::Admincmd(Box::new(cmd))),
            })
            .await
            .map_err(Error::Status)
    }
}
