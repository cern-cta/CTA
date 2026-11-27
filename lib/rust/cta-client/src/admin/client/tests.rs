// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! [`AdminClient`] against a mock frontend serving both admin services on the loopback interface.

use std::mem::discriminant;

use cta_grpc_common::{EndpointConfig, Error as RpcError, JwtAuth};
use cta_protobuf::cta::{
    admin::{AdminCmd, TapePoolLsItem},
    xrd::{
        Data as XrdData, StreamResponse, data::Data, response::ResponseType,
        stream_response::Contents,
    },
};
use tokio_stream::StreamExt;
use tonic::Status;
use url::Url;

use super::AdminClient;
use crate::admin::{
    AdminError,
    commands::{StreamingAdminCmd, drive, repack, tape_pool},
    test_support::*,
};

/// The records of a streaming command
async fn collect_records<C: StreamingAdminCmd>(client: &AdminClient, cmd: C) -> Vec<C::Item> {
    client
        .stream(cmd)
        .await
        .expect("the command should succeed")
        .collect::<Result<Vec<_>, _>>()
        .await
        .expect("the stream should succeed")
}

mod streaming {
    use super::*;

    #[tokio::test]
    async fn tape_pool_ls_gives_typed_records() {
        let items = parse::<TapePoolLsItem>(cta_admin_jsonl!("tape_pools.jsonl"));
        let frames = std::iter::once(success_header())
            .chain(items.iter().cloned().map(|item| data(Data::TplsItem(item))))
            .collect();
        let (client, _) = serve(Reply::Stream(frames, None)).await;

        let output = collect_records(&client, tape_pool::Ls::default()).await;

        assert_eq!(output, items);
    }

    /// A record of another type than the command's is a protocol violation
    #[tokio::test]
    async fn record_of_another_type_is_invalid() {
        let frames = vec![
            success_header(),
            data(Data::TplsItem(TapePoolLsItem::default())),
            data(Data::TplsItem(TapePoolLsItem::default())),
        ];
        let (client, _) = serve(Reply::Stream(frames, None)).await;

        let items: Vec<_> = client
            .stream(drive::Ls::default())
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(items.len(), 1, "{items:#?}");
        let error = items[0].as_ref().unwrap_err();
        assert!(
            matches!(error, AdminError::InvalidResponse { .. }),
            "{error:?}"
        );
        assert!(error.message().starts_with("unexpected record"), "{error}");
    }

    #[tokio::test]
    async fn empty_result() {
        let (client, _) = serve(Reply::Stream(vec![success_header()], None)).await;

        assert!(
            collect_records(&client, tape_pool::Ls::default())
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn skips_frames_without_payload() {
        let frames = vec![
            success_header(),
            StreamResponse {
                contents: Some(Contents::Data(Box::new(XrdData { data: None }))),
            },
            StreamResponse { contents: None },
            data(Data::TplsItem(TapePoolLsItem::default())),
        ];
        let (client, _) = serve(Reply::Stream(frames, None)).await;

        assert_eq!(
            collect_records(&client, tape_pool::Ls::default())
                .await
                .len(),
            1
        );
    }

    /// The refactored frontend reports errors as the status of the call
    #[tokio::test]
    async fn user_error_status() {
        let (client, _) = serve(Reply::StreamFailure(Status::invalid_argument("bad vo"))).await;

        let error = client.stream(tape_pool::Ls::default()).await.err().unwrap();

        assert!(
            matches!(&error, AdminError::User(s) if s.message() == "bad vo"),
            "{error:?}"
        );
    }

    /// The frontend reports errors as the status of the call, the stream header is always a success
    #[tokio::test]
    async fn error_headers_are_invalid() {
        for r#type in [
            ResponseType::RspErrUser,
            ResponseType::RspErrCta,
            ResponseType::RspErrProtobuf,
            ResponseType::RspInvalid,
        ] {
            let (client, _) = serve(Reply::Stream(vec![header(r#type, "msg")], None)).await;

            let error = client.stream(tape_pool::Ls::default()).await.err().unwrap();

            let expected = format!("unexpected response type {}: msg", r#type.as_str_name());
            assert!(
                matches!(error, AdminError::InvalidResponse { .. }),
                "header {type:?}: {error:?}"
            );
            assert_eq!(error.message(), expected);
        }
    }

    #[tokio::test]
    async fn error_after_some_records() {
        let frames = vec![
            success_header(),
            data(Data::TplsItem(TapePoolLsItem::default())),
        ];
        let (client, _) = serve(Reply::Stream(
            frames,
            Some(Status::failed_precondition("catalogue gone")),
        ))
        .await;

        let items: Vec<_> = client
            .stream(tape_pool::Ls::default())
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(items.len(), 2, "{items:#?}");
        assert!(items[0].is_ok());
        let error = items[1].as_ref().unwrap_err();
        assert!(
            matches!(error, AdminError::API(s) if s.message() == "catalogue gone"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn missing_header() {
        for frames in [
            vec![],
            vec![data(Data::TplsItem(TapePoolLsItem::default()))],
        ] {
            let (client, _) = serve(Reply::Stream(frames, None)).await;

            let error = client.stream(tape_pool::Ls::default()).await.err().unwrap();

            assert!(
                matches!(error, AdminError::InvalidResponse { .. }),
                "expected InvalidResponse, got {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn second_header_ends_the_stream() {
        let frames = vec![
            success_header(),
            success_header(),
            data(Data::TplsItem(TapePoolLsItem::default())),
        ];
        let (client, _) = serve(Reply::Stream(frames, None)).await;

        let items: Vec<_> = client
            .stream(tape_pool::Ls::default())
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(items.len(), 1, "{items:#?}");
        assert!(matches!(&items[0], Err(AdminError::InvalidResponse { .. })));
    }
}

mod unary {
    use super::*;

    fn add_cmd() -> tape_pool::Add {
        tape_pool::Add::builder()
            .name("pool")
            .virtual_organisation_name("vo")
            .min_writable_tape_count(2)
            .comment("comment")
            .build()
    }

    #[tokio::test]
    async fn tape_pool_add() {
        let (client, calls) = serve(Reply::Unary(Ok(response(
            ResponseType::RspSuccess,
            "added",
        ))))
        .await;

        let response = client.unary(add_cmd()).await.unwrap();

        assert_eq!(response.message_txt, "added");
        let call = single_call(&calls);
        assert_eq!(call.rpc, "Admin");
        assert_eq!(call.cmd, AdminCmd::from(add_cmd()));
        assert_eq!(call.authorization.as_deref(), Some("Bearer a.token.value"));
    }

    #[tokio::test]
    async fn tape_pool_add_already_exists() {
        let (client, _) = serve(Reply::Unary(Err(Status::invalid_argument(
            "Cannot add tape pool pool because it already exists",
        ))))
        .await;

        let error = client.unary(add_cmd()).await.unwrap_err();

        assert!(
            matches!(
                &error,
                AdminError::User(s) if s.message() == "Cannot add tape pool pool because it already exists"
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn status_codes() {
        let transport = |status| AdminError::Transport(RpcError::Status(status));
        let table = [
            (
                Status::unimplemented("repack disabled"),
                AdminError::CommandDisabled as fn(Status) -> AdminError,
            ),
            (Status::failed_precondition("db error"), AdminError::API),
            (Status::unknown("oops"), AdminError::API),
            (
                Status::unauthenticated("bad token"),
                AdminError::Unauthenticated,
            ),
            (Status::permission_denied("not an admin"), AdminError::API),
            (Status::cancelled("gone"), AdminError::Cancelled),
            (Status::unavailable("restarting"), transport),
        ];
        for (status, expected) in table {
            let expected = expected(status.clone());
            let code = status.code();
            let (client, _) = serve(Reply::Unary(Err(status))).await;

            let error = client
                .unary(repack::Rm::builder().vid("V1").build())
                .await
                .unwrap_err();

            assert_eq!(
                discriminant(&error),
                discriminant(&expected),
                "status {code:?}: {error:?}"
            );
            assert_eq!(error.message(), expected.message());
            assert_eq!(error.status().map(Status::code), Some(code));
            // the status is the source, wrapped in a `cta_grpc_common::Error` for transport errors
            assert!(std::error::Error::source(&error).is_some(), "{error:?}");
        }
    }

    /// `INTERNAL` is raised by the gRPC layers for undecodable messages, never by the frontend
    #[tokio::test]
    async fn internal_status_is_invalid_response() {
        let (client, _) = serve(Reply::Unary(Err(Status::internal("failed to decode")))).await;

        let error = client.unary(add_cmd()).await.unwrap_err();

        assert!(
            matches!(error, AdminError::InvalidResponse { .. }),
            "{error:?}"
        );
        assert_eq!(error.message(), "failed to decode");
        assert!(error.status().is_none());
        let source = std::error::Error::source(&error).unwrap();
        assert_eq!(
            source.downcast_ref::<Status>().unwrap().code(),
            tonic::Code::Internal
        );
    }

    /// The frontend reports errors as the status of the call, a response is always a success
    #[tokio::test]
    async fn error_response_types_are_invalid() {
        for r#type in [
            ResponseType::RspErrUser,
            ResponseType::RspErrCta,
            ResponseType::RspErrProtobuf,
            ResponseType::RspInvalid,
        ] {
            let (client, _) = serve(Reply::Unary(Ok(response(r#type, "msg")))).await;

            let error = client.unary(add_cmd()).await.unwrap_err();

            let expected = format!("unexpected response type {}: msg", r#type.as_str_name());
            assert!(
                matches!(error, AdminError::InvalidResponse { .. }),
                "response type {type:?}: {error:?}"
            );
            assert_eq!(error.message(), expected);
        }
    }
}

#[tokio::test]
async fn connect_to_unreachable_frontend() {
    let config = EndpointConfig::new(
        Url::parse("http://127.0.0.1:1").unwrap(),
        JwtAuth::new(TOKEN.as_bytes().to_vec()).unwrap(),
        None,
        None,
    );

    let error = AdminClient::connect(&config).await.err().unwrap();

    assert!(matches!(error, AdminError::Transport { .. }), "{error:?}");
    assert!(std::error::Error::source(&error).is_some(), "{error:?}");
    // the display names the failed step, the chain of sources holds the reason
    assert_eq!(error.to_string(), "gRPC Transport Error: transport error");
    let mut chain = Vec::new();
    let mut source = std::error::Error::source(&error);
    while let Some(e) = source {
        chain.push(e.to_string());
        source = e.source();
    }
    assert!(
        chain.iter().any(|m| m.contains("Connection refused")),
        "{chain:?}"
    );
}

#[test]
fn display_shows_kind_and_message() {
    let error: AdminError = Status::invalid_argument("bad vo").into();

    assert_eq!(error.to_string(), "User error: bad vo");
}

/// [`AdminClient::stream_json`]: the records as the JSON printed by `cta-admin --jsonl`
#[cfg(feature = "json")]
mod json {
    use cta_protobuf::cta::admin::{
        ShowQueuesItem,
        admin_cmd::{Cmd, SubCmd},
        option_string::Key as StrKey,
    };

    use super::*;
    use crate::admin::commands::show_queues;

    /// The records of a streaming command, as `cta-admin --jsonl` JSON
    async fn collect_json<C>(client: &AdminClient, cmd: C) -> Vec<serde_json::Value>
    where
        C: StreamingAdminCmd,
        C::Item: serde::Serialize,
    {
        client
            .stream_json(cmd)
            .await
            .expect("the command should succeed")
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("the stream should succeed")
    }

    #[tokio::test]
    async fn tape_pool_ls_gives_cta_admin_json() {
        let jsonl = cta_admin_jsonl!("tape_pools.jsonl");
        let frames = std::iter::once(success_header())
            .chain(
                parse::<TapePoolLsItem>(jsonl)
                    .into_iter()
                    .map(|item| data(Data::TplsItem(item))),
            )
            .collect();
        let (client, calls) = serve(Reply::Stream(frames, None)).await;

        let output = collect_json(&client, tape_pool::Ls::builder().vo("ATLAS").build()).await;

        assert_eq!(output, parse::<serde_json::Value>(jsonl));
        let call = single_call(&calls);
        assert_eq!(call.rpc, "GenericAdminStream");
        assert_eq!(
            (call.cmd.cmd(), call.cmd.subcmd()),
            (Cmd::Tapepool, SubCmd::SubcmdLs)
        );
        assert_eq!(call.cmd.option_str.len(), 1);
        assert_eq!(call.cmd.option_str[0].key(), StrKey::Vo);
        assert_eq!(call.cmd.option_str[0].value, "ATLAS");
        assert_eq!(call.authorization.as_deref(), Some("Bearer a.token.value"));
    }

    #[tokio::test]
    async fn show_queues_gives_cta_admin_json() {
        let jsonl = cta_admin_jsonl!("requests.jsonl");
        let frames = std::iter::once(success_header())
            .chain(
                parse::<ShowQueuesItem>(jsonl)
                    .into_iter()
                    .map(|item| data(Data::SqItem(item))),
            )
            .collect();
        let (client, calls) = serve(Reply::Stream(frames, None)).await;

        let output = collect_json(&client, show_queues::ShowQueues::default()).await;

        assert_eq!(output, parse::<serde_json::Value>(jsonl));
        let call = single_call(&calls);
        assert_eq!(call.rpc, "GenericAdminStream");
        assert_eq!(
            (call.cmd.cmd(), call.cmd.subcmd()),
            (Cmd::Showqueues, SubCmd::SubcmdNone)
        );
    }
}
