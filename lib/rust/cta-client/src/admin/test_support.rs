// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Helpers shared by the tests: a mock frontend serving both admin services on the loopback interface, and the
//! `cta-admin --jsonl` samples of the `cta-sdk` unit tests.

use std::sync::{Arc, Mutex};

use cta_grpc_common::{EndpointConfig, JwtAuth};
use cta_protobuf::cta::xrd::{
    Data as XrdData, Request, Response, StreamResponse,
    cta_rpc_server::{CtaRpc, CtaRpcServer},
    cta_rpc_stream_server::{CtaRpcStream, CtaRpcStreamServer},
    data::Data,
    request::Request as RequestType,
    response::ResponseType,
    stream_response::Contents,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Status, transport::Server};
use url::Url;

use crate::admin::AdminClient;

pub(crate) const TOKEN: &str = "a.token.value";

pub(crate) use cta_model::test_support::{cta_admin_jsonl, parse};

/// What the mock frontend answers to an admin command.
#[derive(Clone)]
pub(crate) enum Reply {
    /// Answer of `CtaRpc.Admin`
    Unary(Result<Response, Status>),
    /// Frames of `CtaRpcStream.GenericAdminStream`, then the final status (`None`: success)
    Stream(Vec<StreamResponse>, Option<Status>),
    /// `CtaRpcStream.GenericAdminStream` failing before sending anything
    StreamFailure(Status),
}

/// A call received by the mock frontend.
#[derive(Debug, Clone)]
pub(crate) struct Call {
    pub(crate) rpc: &'static str,
    pub(crate) cmd: cta_protobuf::cta::admin::AdminCmd,
    pub(crate) authorization: Option<String>,
}

/// A frontend giving the same reply to every admin command, and recording the calls.
#[derive(Clone)]
pub(crate) struct MockFrontend {
    reply: Reply,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl MockFrontend {
    fn record(&self, rpc: &'static str, request: &tonic::Request<Request>) {
        let Some(RequestType::Admincmd(cmd)) = &request.get_ref().request else {
            panic!("not an admin command: {request:?}");
        };
        let authorization = request
            .metadata()
            .get("authorization")
            .map(|v| v.to_str().unwrap().to_owned());
        self.calls.lock().unwrap().push(Call {
            rpc,
            cmd: (**cmd).clone(),
            authorization,
        });
    }
}

#[tonic::async_trait]
impl CtaRpc for MockFrontend {
    async fn generic_request(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not an admin RPC"))
    }
    async fn create(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not an admin RPC"))
    }
    async fn archive(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not an admin RPC"))
    }
    async fn retrieve(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not an admin RPC"))
    }
    async fn delete(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not an admin RPC"))
    }
    async fn cancel_retrieve(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not an admin RPC"))
    }

    async fn admin(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        self.record("Admin", &request);
        match &self.reply {
            Reply::Unary(reply) => reply.clone().map(tonic::Response::new),
            // what the refactored frontend answers to a streaming command sent to `Admin`
            Reply::Stream(..) | Reply::StreamFailure(_) => {
                Err(Status::failed_precondition("not a unary command"))
            }
        }
    }
}

type FrameStream = tokio_stream::Iter<std::vec::IntoIter<Result<StreamResponse, Status>>>;

#[tonic::async_trait]
impl CtaRpcStream for MockFrontend {
    type TapeLsStream = FrameStream;
    type GenericAdminStreamStream = FrameStream;

    async fn tape_ls(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<FrameStream>, Status> {
        Err(Status::unimplemented("TapeLs is not implemented"))
    }

    async fn generic_admin_stream(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<FrameStream>, Status> {
        self.record("GenericAdminStream", &request);
        match &self.reply {
            Reply::Stream(frames, status) => {
                let items: Vec<_> = frames
                    .iter()
                    .cloned()
                    .map(Ok)
                    .chain(status.clone().map(Err))
                    .collect();
                Ok(tonic::Response::new(tokio_stream::iter(items)))
            }
            Reply::StreamFailure(status) => Err(status.clone()),
            // what the refactored frontend answers to a unary command sent to `GenericAdminStream`
            Reply::Unary(_) => Err(Status::invalid_argument("not a streaming command")),
        }
    }
}

/// Starts a mock frontend answering `reply`, and returns a client connected to it with the recorded calls.
pub(crate) async fn serve(reply: Reply) -> (AdminClient, Arc<Mutex<Vec<Call>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let frontend = MockFrontend {
        reply,
        calls: calls.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(CtaRpcServer::new(frontend.clone()))
            .add_service(CtaRpcStreamServer::new(frontend))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let config = EndpointConfig::new(
        Url::parse(&format!("http://{address}")).unwrap(),
        JwtAuth::new(TOKEN.as_bytes().to_vec()).unwrap(),
        None,
        None,
    );
    let client = AdminClient::connect(&config).await.unwrap();
    (client, calls)
}

pub(crate) fn header(r#type: ResponseType, message: &str) -> StreamResponse {
    StreamResponse {
        contents: Some(Contents::Header(Box::new(Response {
            r#type: r#type.into(),
            message_txt: message.to_owned(),
            ..Default::default()
        }))),
    }
}

pub(crate) fn success_header() -> StreamResponse {
    header(ResponseType::RspSuccess, "")
}

pub(crate) fn data(data: Data) -> StreamResponse {
    StreamResponse {
        contents: Some(Contents::Data(Box::new(XrdData { data: Some(data) }))),
    }
}

pub(crate) fn response(r#type: ResponseType, message: &str) -> Response {
    Response {
        r#type: r#type.into(),
        message_txt: message.to_owned(),
        ..Default::default()
    }
}

/// The single call recorded by the mock frontend.
pub(crate) fn single_call(calls: &Mutex<Vec<Call>>) -> Call {
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "expected one call, got {calls:#?}");
    calls[0].clone()
}
