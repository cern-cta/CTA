// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Transport-level plumbing shared by the CTA and EOS clients.
//!
//! The central type is [`EndpointConfig`]: it describes *where* to connect
//! (a `http://` or `https://` [`Url`]), *how* to authenticate ([`JwtAuth`]) and
//! which TLS trust material to use. [`EndpointConfig::build_channel`] turns that
//! description into a connected [`Channel`], and
//! [`AuthorizationInterceptor`] attaches the bearer token to every outgoing
//! request.

use std::{fs, path::PathBuf};

use tonic::{
    metadata::{MetadataValue, errors::InvalidMetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig},
};
use url::Url;

/// A JSON Web Token used to authenticate against CTA and EOS.
#[derive(Debug, Clone)]
pub struct JwtAuth {
    /// The serialized token, as read from disk.
    pub token: Vec<u8>,
}

impl JwtAuth {
    /// Wraps the raw bytes of a token.
    pub fn new(token: Vec<u8>) -> Self {
        Self { token }
    }
}

/// Everything needed to open an authenticated gRPC channel to a service.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Service endpoint (`http://` or `https://`)
    pub endpoint: Url,
    /// Credentials presented to the service (only JWT supported for now)
    pub authentication: JwtAuth,
    /// Optional PEM bundle with additional CA certificates to trust. When
    /// unset, the platform trust store is used.
    pub ca_cert_bundle: Option<PathBuf>,
    /// Optional hostname to validate the server certificate against, for cases
    /// where the connection is made to an address that does not match the
    /// certificate's subject (e.g. a port-forwarded development cluster).
    pub alternative_cta_hostname: Option<String>,
}

/// Errors raised while configuring or using a gRPC connection.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// A local file could not be read.
    #[error("IO Error: {0}")]
    IO(std::io::Error),
    /// The remote service returned an error status for a call.
    #[error("gRPC Status Error: {0:?}")]
    Status(tonic::Status),
    /// The channel could not be established, or TLS could not be configured.
    #[error("gRPC Transport Error: {0:?}")]
    Transport(tonic::transport::Error),
    /// The token could not be decoded as a usable credential.
    #[error("Corrupted token value: {0}")]
    CorruptedToken(String),
    /// The token cannot be represented as a gRPC metadata value.
    #[error("Invalid Metadata: {0}")]
    InvalidMetadata(InvalidMetadataValue),
    /// The configured endpoint is not a valid gRPC URI.
    #[error("Invalid URI: {0}")]
    InvalidURI(String),
}

impl EndpointConfig {
    /// Assembles a configuration from its parts.
    ///
    /// See the field documentation of [`EndpointConfig`] for the meaning of the
    /// optional arguments.
    pub fn new(
        endpoint: Url,
        authentication: JwtAuth,
        ca_cert_bundle: Option<PathBuf>,
        alternative_cta_hostname: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            authentication,
            ca_cert_bundle,
            alternative_cta_hostname,
        }
    }

    /// Connects to [`Self::endpoint`] and returns the resulting channel.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidURI`] if the endpoint cannot be used as a gRPC
    /// URI, [`Error::IO`] if the CA bundle cannot be read and
    /// [`Error::Transport`] if TLS setup or the connection itself fails.
    pub async fn build_channel(&self) -> Result<Channel, Error> {
        let insecure = self.endpoint.scheme() == "http";

        let endpoint = Channel::from_shared(self.endpoint.to_string())
            .map_err(|_| Error::InvalidURI(self.endpoint.clone().into()))?;

        let endpoint = if insecure {
            endpoint
        } else {
            let mut tls: ClientTlsConfig = ClientTlsConfig::new();

            if let Some(file_path) = &self.ca_cert_bundle {
                let data = fs::read(file_path).map_err(Error::IO)?;
                tls = tls.ca_certificate(Certificate::from_pem(data));
            }

            if let Some(hostname) = &self.alternative_cta_hostname {
                tls = tls.domain_name(hostname);
            }

            endpoint.tls_config(tls).map_err(Error::Transport)?
        };

        let channel = endpoint.connect().await.map_err(Error::Transport)?;

        Ok(channel)
    }
}

/// A [`tonic`] interceptor that adds `authorization: Bearer <token>` to every
/// request sent through the intercepted service.
pub struct AuthorizationInterceptor {
    token: MetadataValue<tonic::metadata::Ascii>,
}

impl AuthorizationInterceptor {
    /// Pre-renders the authorization header from the given credentials.
    ///
    /// # Panics
    ///
    /// Panics if the token is not valid UTF-8 or cannot be represented as an
    /// ASCII metadata value.
    pub fn new(auth: crate::rpc::JwtAuth) -> Self {
        let token: MetadataValue<_> = format!(
            "Bearer {}",
            str::from_utf8(&auth.token).expect("Token is not valid UTF-8")
        )
        .parse()
        .expect("Token is not valid metadata");

        Self { token }
    }
}

impl Interceptor for AuthorizationInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        log::trace!("Adding authorization: {:#?}", self.token);
        req.metadata_mut()
            .insert("authorization", self.token.clone());
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoint: &str) -> EndpointConfig {
        EndpointConfig::new(
            Url::parse(endpoint).expect("test endpoint should parse"),
            JwtAuth::new(b"a.token.value".to_vec()),
            None,
            None,
        )
    }

    #[test]
    fn interceptor_adds_a_bearer_authorization_header() {
        let mut interceptor =
            AuthorizationInterceptor::new(JwtAuth::new(b"a.token.value".to_vec()));

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
    #[should_panic(expected = "Token is not valid UTF-8")]
    fn interceptor_rejects_a_non_utf8_token() {
        let _ = AuthorizationInterceptor::new(JwtAuth::new(vec![0xff, 0xfe]));
    }

    #[test]
    #[should_panic(expected = "Token is not valid metadata")]
    fn interceptor_rejects_a_token_with_control_characters() {
        let _ = AuthorizationInterceptor::new(JwtAuth::new(b"line\nbreak".to_vec()));
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

    #[test]
    fn error_messages_name_their_cause() {
        assert_eq!(
            Error::InvalidURI("not-a-uri".into()).to_string(),
            "Invalid URI: not-a-uri"
        );
        assert_eq!(
            Error::CorruptedToken("empty".into()).to_string(),
            "Corrupted token value: empty"
        );
    }
}
