// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use std::{path::PathBuf, string::FromUtf8Error};

use tonic::service::Interceptor;
use url::Url;

use crate::{
    AuthorizationInterceptor, EndpointConfig, Error, JwtAuth, errors::SUPPORTED_SCHEMES,
    validate_scheme,
};

fn config(endpoint: &str) -> EndpointConfig {
    EndpointConfig::new(
        Url::parse(endpoint).expect("test endpoint should parse"),
        JwtAuth::new(b"a.token.value".to_vec()).unwrap(),
        None,
        None,
    )
}

#[test]
fn jwt_auth_rejects_a_non_utf8_token() {
    let error = JwtAuth::new(vec![0xff, 0xfe]).expect_err("a non-UTF-8 token should be rejected");

    assert!(
        matches!(&error, FromUtf8Error { .. }),
        "expected a FromUtf8Error error, got {error:?}"
    );
}

#[test]
fn interceptor_adds_a_bearer_authorization_header() {
    let mut interceptor =
        AuthorizationInterceptor::new(JwtAuth::new(b"a.token.value".to_vec()).unwrap())
            .expect("a plain token should be accepted");

    let request = interceptor
        .call(tonic::Request::new(()))
        .expect("interceptor should not fail");

    assert_eq!(
        request
            .metadata()
            .get("authorization")
            .map(|v| v.to_str().unwrap()),
        Some("Bearer a.token.value")
    );
}

#[test]
fn interceptor_rejects_a_token_with_control_characters() {
    let error = AuthorizationInterceptor::new(JwtAuth::new(b"line\nbreak".to_vec()).unwrap())
        .err()
        .expect("a token with a line break should be rejected");

    assert!(
        matches!(error, Error::InvalidMetadata(_)),
        "expected an InvalidMetadata error, got {error:?}"
    );
}

#[test]
fn validate_scheme_accepts_http_and_https() {
    for scheme in SUPPORTED_SCHEMES {
        let url = Url::parse(&format!("{scheme}://frontend.example.org:17017")).unwrap();
        assert!(validate_scheme(&url).is_ok(), "{scheme} should be accepted");
    }
}

#[test]
fn validate_scheme_rejects_anything_else() {
    for endpoint in ["grpc://frontend.example.org", "file:///tmp/socket"] {
        let url = Url::parse(endpoint).unwrap();
        let error = validate_scheme(&url).expect_err("{endpoint} should be rejected");

        assert!(
            matches!(error, Error::UnsupportedScheme(_)),
            "expected an UnsupportedScheme error, got {error:?}"
        );
    }
}

#[tokio::test]
async fn build_channel_rejects_an_unsupported_scheme() {
    // Must fail on the scheme rather than silently attempting TLS.
    let error = config("grpc://frontend.example.org:17017")
        .build_channel()
        .await
        .expect_err("an unsupported scheme should be an error");

    assert!(
        matches!(error, Error::UnsupportedScheme(ref s) if s == "grpc"),
        "expected an UnsupportedScheme error, got {error:?}"
    );
}

#[tokio::test]
async fn build_channel_reports_a_missing_ca_bundle() {
    let mut config = config("https://frontend.example.org:50051");
    config.ca_cert_bundle = Some(PathBuf::from("/nonexistent/ca-bundle.pem"));

    // Fails before any connection attempt, so this test touches no network.
    let error = config
        .build_channel()
        .await
        .expect_err("a missing CA bundle should be an error");

    assert!(
        matches!(error, Error::IO(_)),
        "expected an IO error, got {error:?}"
    );
}

#[tokio::test]
async fn build_channel_reports_an_unreachable_endpoint() {
    // Port 1 on the loopback interface is never listening.
    let error = config("http://127.0.0.1:1")
        .build_channel()
        .await
        .expect_err("connecting to a closed port should fail");

    assert!(
        matches!(error, Error::Transport(_)),
        "expected a Transport error, got {error:?}"
    );
}
