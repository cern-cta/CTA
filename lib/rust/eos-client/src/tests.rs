// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use std::collections::HashMap;

use bytes::Bytes;
use cta_grpc_common::{
    EndpointConfig, JwtAuth,
    test_utils::{corrupt_streaming_response, streaming_response, streaming_response_from_bytes},
    utils::system_time_now,
};
use eos_protobuf::eos::rpc::{MdId, MdRequest, MdResponse, Type};
use nix::sys::stat::Mode;
use tokio_stream::StreamExt;
use tonic::Status;
use url::Url;

use crate::{DEFAULT_FILE_MODE, EosEndpointMap, Error, with_auth_key_from};

fn endpoint_config() -> EndpointConfig {
    EndpointConfig::new(
        Url::parse("http://127.0.0.1:1").expect("test endpoint should parse"),
        JwtAuth::new(b"token".to_vec()).unwrap(),
        None,
        None,
    )
}

fn endpoint_map(instances: &[&str]) -> EosEndpointMap {
    EosEndpointMap::from(
        instances
            .iter()
            .map(|name| ((*name).to_string(), endpoint_config()))
            .collect::<HashMap<_, _>>(),
    )
}

fn file_id(id: u64) -> MdId {
    MdId {
        r#type: Type::File.into(),
        id,
        path: Vec::new(),
        ino: 0,
    }
}

mod default_file_mode {
    use super::*;

    #[test]
    fn grants_expected_permissions() {
        let mode = *DEFAULT_FILE_MODE;

        for granted in [
            Mode::S_IRUSR,
            Mode::S_IWUSR,
            Mode::S_IXUSR,
            Mode::S_IRGRP,
            Mode::S_IWGRP,
            Mode::S_IROTH,
            Mode::S_IWOTH,
        ] {
            assert!(mode.contains(granted), "{granted:?} should be set");
        }

        // EOS does not follow POSIX semantics for these, so they must be clear.
        for cleared in [Mode::S_ISUID, Mode::S_ISGID, Mode::S_ISVTX] {
            assert!(!mode.intersects(cleared), "{cleared:?} should be cleared");
        }

        assert_eq!(mode.bits(), 0o766, "rwxrw-rw-");
    }
}

mod system_time_now {
    use super::*;

    #[test]
    fn is_plausible() {
        let now = system_time_now();

        // 2026-01-01T00:00:00Z; the clock of a machine running CTA is past that.
        assert!(now > 1_767_225_600, "{now} should be a recent timestamp");
    }
}

mod auth_key_macro {
    use super::*;

    #[test]
    fn fills_in_token() {
        let auth = JwtAuth::new(b"a.token.value".to_vec()).unwrap();

        let request = with_auth_key_from!(
            auth,
            MdRequest {
                r#type: Type::File.into(),
                id: Some(file_id(42)),
                role: None,
                selection: None,
            }
        );

        assert_eq!(request.authkey, "a.token.value");
        assert_eq!(request.id.expect("id should be set").id, 42);
    }
}

mod endpoint_map {
    use super::*;

    #[tokio::test]
    async fn unknown_instance_is_not_found() {
        let mut map = endpoint_map(&["eosctatape"]);

        match map.get_client("does-not-exist").await {
            Err(Error::DiskInstanceNotFound(instance)) => assert_eq!(instance, "does-not-exist"),
            other => panic!("expected DiskInstanceNotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn proxy_methods_report_unknown_instance() {
        let mut map = endpoint_map(&["eosctatape"]);

        let error = map
            .get_current_ids("does-not-exist")
            .await
            .expect_err("unknown disk instance should be an error");

        assert!(matches!(error, Error::DiskInstanceNotFound(ref i) if i == "does-not-exist"));
    }

    #[tokio::test]
    async fn connection_failure_is_rpc_error() {
        let mut map = endpoint_map(&["eosctatape"]);

        let error = map
            .get_current_ids("eosctatape")
            .await
            .expect_err("unreachable endpoint should error");

        assert!(
            matches!(error, Error::Rpc(_)),
            "expected Rpc error, got {error:?}"
        );
    }
}

mod eos_stream {
    use super::*;

    #[tokio::test]
    async fn yields_every_response_in_order() {
        let responses = [
            MdResponse {
                r#type: 0,
                ..Default::default()
            },
            MdResponse {
                r#type: 1,
                ..Default::default()
            },
        ];
        let response = streaming_response(&responses);

        let items: Vec<_> = response
            .collect::<Result<Vec<_>, _>>()
            .await
            .expect("stream should succeed");

        assert_eq!(items, responses);
    }

    #[tokio::test]
    async fn propagates_decoding_failure() {
        let mut response = corrupt_streaming_response::<MdResponse>();

        let first: Option<Result<MdResponse, Status>> = response.next().await;

        assert!(
            matches!(first, Some(Err(_))),
            "expected a Status error, got {first:#?}"
        );
    }

    #[tokio::test]
    async fn empty_body_terminates() {
        let mut response = streaming_response_from_bytes::<MdResponse>(Bytes::new());

        assert!(response.next().await.is_none());
    }
}
