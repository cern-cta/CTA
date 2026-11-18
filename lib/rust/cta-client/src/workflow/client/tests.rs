// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! [`WorkflowClient`] against a mock workflow API on the loopback interface.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use cta_grpc_common::{EndpointConfig, JwtAuth};
use cta_model::{
    events::{
        Archive, CancelRetrieve, Create, Delete, FileInfo, InvalidEvent, Requester, Retrieve,
    },
    file::{Checksum, ChecksumType, Owner},
};
use cta_protobuf::cta::xrd::{
    Request, Response,
    cta_rpc_server::{CtaRpc, CtaRpcServer},
    response::ResponseType,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Status, transport::Server};
use url::Url;

use super::*;

const TOKEN: &str = "a.token.value";

/// A call received by the mock workflow API.
#[derive(Debug, Clone)]
struct Call {
    rpc: &'static str,
    request: Request,
    authorization: Option<String>,
    timeout: Option<String>,
}

/// A workflow API giving the same reply to every event, and recording the calls.
#[derive(Clone)]
struct MockWorkflowApi {
    reply: Result<Response, Status>,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl MockWorkflowApi {
    fn answer(
        &self,
        rpc: &'static str,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        let header = |name| {
            request
                .metadata()
                .get(name)
                .map(|v| v.to_str().unwrap().to_owned())
        };
        let (authorization, timeout) = (header("authorization"), header("grpc-timeout"));
        self.calls.lock().unwrap().push(Call {
            rpc,
            request: request.into_inner(),
            authorization,
            timeout,
        });
        self.reply.clone().map(tonic::Response::new)
    }
}

#[tonic::async_trait]
impl CtaRpc for MockWorkflowApi {
    async fn generic_request(
        &self,
        _: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not a workflow RPC"))
    }
    async fn admin(&self, _: tonic::Request<Request>) -> Result<tonic::Response<Response>, Status> {
        Err(Status::unimplemented("not a workflow RPC"))
    }
    async fn create(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        self.answer("Create", request)
    }
    async fn archive(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        self.answer("Archive", request)
    }
    async fn retrieve(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        self.answer("Retrieve", request)
    }
    async fn delete(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        self.answer("Delete", request)
    }
    async fn cancel_retrieve(
        &self,
        request: tonic::Request<Request>,
    ) -> Result<tonic::Response<Response>, Status> {
        self.answer("CancelRetrieve", request)
    }
}

fn success() -> Result<Response, Status> {
    Ok(Response {
        r#type: ResponseType::RspSuccess.into(),
        message_txt: "OK".to_owned(),
        ..Response::default()
    })
}

/// Starts a mock workflow API answering `reply`, and returns a client of the disk instance `ctaeos` connected to it,
/// with the recorded calls.
async fn serve(reply: Result<Response, Status>) -> (WorkflowClient, Arc<Mutex<Vec<Call>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let api = MockWorkflowApi {
        reply,
        calls: calls.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(CtaRpcServer::new(api))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let config = EndpointConfig::new(
        Url::parse(&format!("http://{address}")).unwrap(),
        JwtAuth::new(TOKEN.as_bytes().to_vec()).unwrap(),
        None,
        None,
    );
    let client = WorkflowClient::connect(&config, "ctaeos").await.unwrap();
    (client, calls)
}

fn single_call(calls: &Mutex<Vec<Call>>) -> Call {
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "expected one call, got {calls:#?}");
    calls[0].clone()
}

fn requester() -> Requester {
    Requester::new("someuser", "somegroup")
}

/// A file with every field set
fn full_file() -> FileInfo {
    FileInfo::builder()
        .archive_file_id(123)
        .disk_file_id("42")
        .path("/eos/ctaeos/file")
        .owner(Owner(1000, 1001))
        .size(4096)
        .checksums(vec![Checksum::new(
            ChecksumType::Adler32,
            0x0a0b_0c0d_u32.to_le_bytes().to_vec(),
        )])
        .xattr(BTreeMap::from([
            ("sys.archive.file_id".to_owned(), "123".to_owned()),
            ("user.key".to_owned(), "value".to_owned()),
        ]))
        .build()
}

/// Sends `event`, and checks that it reaches the RPC `rpc` with its event type, authenticated.
async fn assert_sent_to<E: WorkflowEvent>(event: E, rpc: &str) {
    let (client, calls) = serve(success()).await;

    let reply = client.send(&event).await.expect("the event should succeed");

    let call = single_call(&calls);
    assert_eq!(call.rpc, rpc);
    assert_eq!(call.request, reply.request.request());
    let wf = reply.request.notification().wf.as_ref().unwrap();
    assert_eq!(wf.event(), E::EVENT_TYPE);
    assert_eq!(call.authorization.as_deref(), Some("Bearer a.token.value"));
}

#[tokio::test]
async fn events_are_sent_to_their_rpc() {
    let file = full_file;
    assert_sent_to(
        Create::builder()
            .file(file())
            .storage_class("SC")
            .requester(requester())
            .build(),
        "Create",
    )
    .await;
    assert_sent_to(
        Archive::builder()
            .file(file())
            .storage_class("SC")
            .requester(requester())
            .build(),
        "Archive",
    )
    .await;
    assert_sent_to(
        Retrieve::builder()
            .file(file())
            .requester(requester())
            .dst_url("root://disk/dst")
            .build(),
        "Retrieve",
    )
    .await;
    assert_sent_to(
        CancelRetrieve::builder()
            .file(file())
            .requester(requester())
            .request_objectstore_id("store-abc")
            .build(),
        "CancelRetrieve",
    )
    .await;
    assert_sent_to(
        Delete::builder()
            .file(file())
            .requester(requester())
            .build(),
        "Delete",
    )
    .await;
}

mod checked_locally {
    use super::*;

    /// Sends `event`, and checks that it fails with `expected` without reaching the workflow API.
    async fn assert_rejected<E: WorkflowEvent>(event: E, expected: InvalidEvent) {
        let (client, calls) = serve(success()).await;

        let error = client
            .send(&event)
            .await
            .expect_err("the event should be rejected");

        match error {
            WorkflowError::InvalidEvent(error) => assert_eq!(error, expected),
            other => panic!("expected an invalid event, got {other:?}"),
        }
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn archive_without_storage_class() {
        let event = Archive::builder()
            .file(full_file())
            .storage_class("")
            .requester(requester())
            .build();
        assert_rejected(event, InvalidEvent::MissingStorageClass).await;
    }

    #[tokio::test]
    async fn retrieve_without_archive_file_id() {
        let event = Retrieve::builder()
            .file(FileInfo::builder().disk_file_id("1").build())
            .requester(requester())
            .dst_url("root://disk/dst")
            .build();
        assert_rejected(event, InvalidEvent::MissingArchiveFileId).await;
    }

    #[tokio::test]
    async fn cancel_retrieve_with_zero_archive_file_id() {
        let event = CancelRetrieve::builder()
            .file(FileInfo::builder().archive_file_id(0).build())
            .requester(requester())
            .request_objectstore_id("store-abc")
            .build();
        assert_rejected(event, InvalidEvent::MissingArchiveFileId).await;
    }

    #[tokio::test]
    async fn delete_without_archive_file_id() {
        let event = Delete::builder()
            .file(FileInfo::default())
            .requester(requester())
            .build();
        assert_rejected(event, InvalidEvent::MissingArchiveFileId).await;
    }
}

mod errors {
    use super::*;

    /// Whether an error is of the expected kind
    type Check = fn(&WorkflowError) -> bool;

    fn delete() -> Delete {
        Delete::builder()
            .file(FileInfo::builder().archive_file_id(1).build())
            .requester(requester())
            .build()
    }

    async fn error_for(reply: Result<Response, Status>) -> WorkflowError {
        let (client, _) = serve(reply).await;
        client
            .send(&delete())
            .await
            .expect_err("the event should fail")
    }

    #[tokio::test]
    async fn statuses() {
        let cases: [(Code, Check); 6] = [
            (Code::Aborted, |e| matches!(e, WorkflowError::User(_))),
            (Code::InvalidArgument, |e| {
                matches!(e, WorkflowError::InvalidRequest(_))
            }),
            (Code::FailedPrecondition, |e| {
                matches!(e, WorkflowError::API(_))
            }),
            (Code::Unknown, |e| matches!(e, WorkflowError::API(_))),
            (Code::Unauthenticated, |e| {
                matches!(e, WorkflowError::Unauthenticated(_))
            }),
            (Code::Unavailable, |e| {
                matches!(e, WorkflowError::Transport(_))
            }),
        ];
        for (code, expected) in cases {
            let error = error_for(Err(Status::new(code, "the reason"))).await;
            assert!(expected(&error), "{code:?} gave {error:?}");
            assert_eq!(error.message(), "the reason");
            assert_eq!(error.status().map(Status::code), Some(code));
        }
    }

    #[tokio::test]
    async fn non_success_response() {
        let reply = Ok(Response {
            r#type: ResponseType::RspErrUser.into(),
            message_txt: "nope".to_owned(),
            ..Response::default()
        });

        let error = error_for(reply).await;

        assert!(
            matches!(&error, WorkflowError::InvalidResponse { message, .. } if message.contains("RSP_ERR_USER: nope")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn unreachable_endpoint() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let config = EndpointConfig::new(
            Url::parse(&format!("http://{address}")).unwrap(),
            JwtAuth::new(TOKEN.as_bytes().to_vec()).unwrap(),
            None,
            None,
        );

        let error = WorkflowClient::connect(&config, "ctaeos")
            .await
            .err()
            .expect("the connection should fail");

        assert!(matches!(error, WorkflowError::Transport(_)), "{error:?}");
    }
}

#[tokio::test]
async fn timeout_is_sent() {
    let (client, calls) = serve(success()).await;
    let delete = Delete::builder()
        .file(FileInfo::builder().archive_file_id(1).build())
        .requester(requester())
        .build();

    client.send(&delete).await.unwrap();
    let client = client.with_timeout(Some(Duration::from_secs(42)));
    client.send(&delete).await.unwrap();
    let client = client.with_timeout(None);
    client.send(&delete).await.unwrap();

    let timeouts: Vec<_> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.timeout.clone())
        .collect();
    // tonic sends the deadline in microseconds
    assert_eq!(
        timeouts,
        [
            Some("30000000u".to_owned()),
            Some("42000000u".to_owned()),
            None
        ]
    );
}

#[cfg(feature = "json")]
#[tokio::test]
async fn request_and_response_as_json() {
    let reply = Ok(Response {
        r#type: ResponseType::RspSuccess.into(),
        message_txt: "OK".to_owned(),
        archive_file_id: "4242".to_owned(),
        ..Response::default()
    });
    let (client, _) = serve(reply).await;
    let event = Create::builder()
        .file(full_file())
        .storage_class("SC")
        .requester(requester())
        .build();

    let request = client.request(&event).unwrap();
    let reply = client.submit(request.clone()).await.unwrap();

    assert_eq!(reply.request, request);
    assert_eq!(reply.message(), "OK");
    let request_json = request.to_json().unwrap();
    assert_eq!(request_json["notification"]["wf"]["event"], "CREATE");
    assert_eq!(request_json["notification"]["file"]["archiveFileId"], "123");
    assert_eq!(
        request_json["notification"]["file"]["csb"]["cs"][0]["value"],
        "DQwLCg=="
    );
    let response_json = reply.response_json().unwrap();
    assert_eq!(response_json["type"], "RSP_SUCCESS");
    assert_eq!(response_json["archiveFileId"], "4242");
}
