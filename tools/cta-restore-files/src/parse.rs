// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Parsing of the namespace keytab file.

use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::PathBuf,
};

use cta_lib::{
    eos::EosEndpointMap,
    rpc::{self, EndpointConfig, JwtAuth},
};
use url::Url;

/// Errors raised while reading the namespace keytab file.
#[derive(Debug, thiserror::Error)]
pub enum KeytabError {
    /// The keytab file could not be opened or read.
    #[error("Failed to open namespace keytab configuration file: {0}")]
    Io(io::Error),
    /// A line has the wrong number of fields, or an endpoint that is not a
    /// valid URL.
    #[error("Could not parse namespace keytab configuration file line {line_no}: {line}")]
    Parse {
        /// One-based line number (as displayed in text editors).
        line_no: usize,
        /// The complete offending line.
        line: String,
    },
    /// The endpoint URL uses a scheme other than `http` or `https`.
    #[error("Unrecognized scheme: {0}. Use either 'http' or 'https'")]
    InvalidScheme(String),
}

impl From<io::Error> for KeytabError {
    fn from(e: io::Error) -> Self {
        KeytabError::Io(e)
    }
}

/// Parses a namespace keytab file into a map of diskInstance -> Namespace.
///
/// Expected line format: `<diskInstance> <endpoint> <token> [<alternativeHost>]`
/// - `#` starts a comment (rest of line ignored)
/// - Blank lines (after stripping comments) are skipped
/// - Any other line must have exactly 3 whitespace-separated fields, plus an
///   optional 4th one: the hostname to verify the TLS certificate against
///
/// # Errors
///
/// Returns [`KeytabError::Io`] if the file cannot be read,
/// [`KeytabError::Parse`] for a malformed line and
/// [`KeytabError::InvalidScheme`] for an endpoint that is neither `http` nor
/// `https`.
pub fn set_namespace_map(
    ca_cert_bundle: Option<PathBuf>,
    keytab_file: &str,
) -> Result<EosEndpointMap, KeytabError> {
    let file = File::open(keytab_file)?;
    let reader = BufReader::new(file);

    let mut endpoint_map = HashMap::new();

    for (lineno, line_result) in reader.lines().enumerate() {
        let raw_line = line_result?;

        // Strip out comments
        let line = match raw_line.find('#') {
            Some(pos) => &raw_line[..pos],
            None => &raw_line[..],
        };

        // Split on whitespace; take up to 4 tokens to detect trailing garbage
        let mut fields = line.split_whitespace();
        let disk_instance = fields.next();
        let endpoint = fields.next();
        let token = fields.next();
        let alternative_host = fields.next();
        let eol = fields.next();

        match (disk_instance, endpoint, token, alternative_host, eol) {
            // Fully blank line (after comment stripping) -> skip
            (None, None, None, None, None) => continue,
            // 3 or 4 fields, nothing trailing -> valid entry
            (Some(d), Some(endpoint), Some(t), host, None) => {
                let endpoint_url = Url::parse(endpoint).map_err(|_e| KeytabError::Parse {
                    line_no: lineno + 1,
                    line: raw_line.clone(),
                })?;

                rpc::validate_scheme(&endpoint_url)
                    .map_err(|_| KeytabError::InvalidScheme(endpoint_url.scheme().to_string()))?;

                endpoint_map.insert(
                    d.to_string(),
                    EndpointConfig::new(
                        endpoint_url,
                        JwtAuth::new(t.as_bytes().into()),
                        ca_cert_bundle.clone(),
                        host.map(|h| h.into()),
                    ),
                );
            }
            // Anything else (1, 2 fields, or more than 3) -> malformed
            _ => {
                return Err(KeytabError::Parse {
                    line_no: lineno + 1,
                    line: raw_line,
                });
            }
        }
    }

    Ok(EosEndpointMap::from(endpoint_map))
}
