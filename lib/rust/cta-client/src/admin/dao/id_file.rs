// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Lists of disk file identifiers read from files.

use std::{fs, io, path::Path};

use crate::admin::AdminError;

/// Reads a list of disk file identifiers from a file.
///
/// The file is made of whitespace-separated tokens, where `#` starts a comment until the end of the line. Only the
/// `fid=<hex>` tokens are kept (with or without a `0x` prefix), and their hexadecimal value is converted to the
/// decimal string the catalogue stores; every other token is ignored. For example:
///
/// ```text
/// # files to restore
/// /data/dir/
/// /data/dir/file.txt fid=8
/// /data/dir/other.txt fid=0x1f
/// fid=2a
/// ```
///
/// gives `["8", "31", "42"]`.
///
/// This format comes originally from `eos find --fid <path>`.
///
/// # Errors
///
/// [`IdFileError::Io`] when the file cannot be read, [`IdFileError::InvalidHex`] when a `fid=` token is not a
/// hexadecimal number.
pub fn read_disk_file_ids(path: impl AsRef<Path>) -> Result<Vec<String>, IdFileError> {
    parse_disk_file_ids(&fs::read_to_string(path)?)
}

/// Same as [`read_disk_file_ids`], on the content of the file.
pub(crate) fn parse_disk_file_ids(content: &str) -> Result<Vec<String>, IdFileError> {
    let mut ids = Vec::new();
    for line in content.lines() {
        let line = line.split('#').next().unwrap_or_default();
        for token in line.split_whitespace() {
            let Some(fid) = token.strip_prefix("fid=") else {
                continue;
            };
            // convert to lowercase to accept 0x and 0X
            let fid = fid.to_ascii_lowercase();

            let digits = fid.strip_prefix("0x").unwrap_or(&fid);
            let id = u64::from_str_radix(digits, 16)
                .map_err(|_| IdFileError::InvalidHex(fid.to_owned()))?;
            ids.push(id.to_string());
        }
    }
    Ok(ids)
}

/// [`read_disk_file_ids`] as an [`AdminError`], for the data access objects.
pub(crate) fn disk_file_ids_from(path: &Path) -> Result<Vec<String>, AdminError> {
    read_disk_file_ids(path).map_err(|source| AdminError::IdFile {
        path: path.to_owned(),
        source,
    })
}

/// A list of identifiers cannot be read from a file (see [`read_disk_file_ids`]).
#[derive(Debug, thiserror::Error)]
pub enum IdFileError {
    /// The file cannot be read
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The file identifier is not a hexadecimal number
    #[error("{0:?} is not a valid hexadecimal file ID")]
    InvalidHex(String),
}
