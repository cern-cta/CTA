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

use std::{fs, path::PathBuf, string::FromUtf8Error};

use tonic::{
    metadata::MetadataValue,
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig},
};
use url::Url;

use crate::errors::{Error, validate_scheme};

/// A JSON Web Token used to authenticate against CTA and EOS.
#[derive(Clone)]
pub struct JwtAuth {
    /// The token as a string
    pub token: String,
}

impl std::fmt::Debug for JwtAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtAuth")
            .field("token", &"[redacted]")
            .finish()
    }
}

impl JwtAuth {
    /// Wraps the raw bytes of a token.
    ///
    /// # Errors
    ///
    /// It can fail if the token is malformed (UTF-8 Error)
    pub fn new(token: Vec<u8>) -> Result<Self, FromUtf8Error> {
        Ok(Self {
            token: String::from_utf8(token)?,
        })
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
    /// Returns [`Error::UnsupportedScheme`] if the endpoint scheme is not one
    /// of `SUPPORTED_SCHEMES`, [`Error::InvalidURI`] if the endpoint cannot
    /// be used as a gRPC URI, [`Error::IO`] if the CA bundle cannot be read and
    /// [`Error::Transport`] if TLS setup or the connection itself fails.
    pub async fn build_channel(&self) -> Result<Channel, Error> {
        // Only `http` is plaintext; every other supported scheme gets TLS.
        validate_scheme(&self.endpoint)?;
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
    /// # Errors
    ///
    /// Returns [`Error::InvalidMetadata`] if the token cannot be represented as an ASCII
    /// metadata value (for example because it contains a line break).
    pub fn new(auth: crate::rpc::JwtAuth) -> Result<Self, Error> {
        let token: MetadataValue<_> = format!("Bearer {}", auth.token).parse()?;

        Ok(Self { token })
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
