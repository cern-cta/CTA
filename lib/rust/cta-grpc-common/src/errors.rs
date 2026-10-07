// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Errors and validation

use tonic::metadata::errors::InvalidMetadataValue;
use url::Url;

/// Errors raised while configuring or using a gRPC connection.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// A local file could not be read.
    #[error("IO Error: {0}")]
    IO(std::io::Error),
    /// The remote service returned an error status for a call.
    #[error("gRPC Status Error: {0:?}")]
    Status(#[from] tonic::Status),
    /// The channel could not be established, or TLS could not be configured.
    #[error("gRPC Transport Error: {0:?}")]
    Transport(tonic::transport::Error),
    /// The token cannot be represented as a gRPC metadata value.
    #[error("Invalid Metadata: {0}")]
    InvalidMetadata(#[from] InvalidMetadataValue),
    /// The configured endpoint is not a valid gRPC URI.
    #[error("Invalid URI: {0}")]
    InvalidURI(String),
    /// The endpoint URL uses a scheme this crate cannot connect to.
    #[error("Unrecognized scheme: {0}. Use either 'http' or 'https'")]
    UnsupportedScheme(String),
}

/// The endpoint URL schemes [`EndpointConfig::build_channel`] can connect to
pub const SUPPORTED_SCHEMES: [&str; 2] = ["http", "https"];

/// Checks that `endpoint` carries a scheme this crate can connect to.
/// # Errors
///
/// Returns [`Error::UnsupportedScheme`] for anything outside
/// `SUPPORTED_SCHEMES`.
pub fn validate_scheme(endpoint: &Url) -> Result<(), Error> {
    let scheme = endpoint.scheme();

    if SUPPORTED_SCHEMES.contains(&scheme) {
        Ok(())
    } else {
        Err(Error::UnsupportedScheme(scheme.to_string()))
    }
}
