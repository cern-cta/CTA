// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use std::{fs, path::PathBuf};

use tonic::{
    metadata::{MetadataValue, errors::InvalidMetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig},
};
use url::Url;

#[derive(Debug, Clone)]
pub struct JwtAuth {
    pub token: Vec<u8>,
}

impl JwtAuth {
    pub fn new(token: Vec<u8>) -> Self {
        Self { token }
    }
}

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub endpoint: Url,
    pub authentication: JwtAuth,
    pub ca_cert_bundle: Option<PathBuf>,
    pub alternative_cta_hostname: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(std::io::Error),
    #[error("gRPC Status Error: {0:?}")]
    Status(tonic::Status),
    #[error("gRPC Transport Error: {0:?}")]
    Transport(tonic::transport::Error),
    #[error("Corrupted token value: {0}")]
    CorruptedToken(String),
    #[error("Invalid Metadata: {0}")]
    InvalidMetadata(InvalidMetadataValue),
    #[error("Invalid URI: {0}")]
    InvalidURI(String),
}

impl EndpointConfig {
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

pub struct AuthorizationInterceptor {
    token: MetadataValue<tonic::metadata::Ascii>,
}

impl AuthorizationInterceptor {
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
