// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Errors of the conversions of the domain types of [`crate::types`]

/// An error type for the conversion of checksum types. Useful to convert between EOS and CTA.
#[derive(Debug, thiserror::Error)]
pub enum ChecksumTypeError<T> {
    /// Error parsing a checksum name string
    #[error("Error parsing string '{0}'")]
    Parse(String),
    /// Error converting from a source checksum value
    #[error("Source checksum value not supported: {0:?}")]
    UnknownType(T),
}

/// Error type for failures during archive file conversion from protobuf.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveFileConversionError {
    /// The archive file creation timestamp was out of valid range
    #[error("Invalid archive file creation timestamp: {0}")]
    InvalidTimestamp(i64),
    /// One or more checksums could not be converted
    #[error("Failed to convert checksum: {0}")]
    ChecksumConversion(String),
}

/// Error type for failures during recycle tape file list item conversion.
#[derive(Debug, thiserror::Error)]
pub enum RecycleTapeFileConversionError {
    /// The archive file creation timestamp was out of valid range
    #[error("Invalid archive file creation timestamp: {0}")]
    InvalidTimestamp(i64),
    /// One or more checksums could not be converted
    #[error("Failed to convert checksum: {0}")]
    ChecksumConversion(String),
}

/// Error returned when a [`cta_protobuf::cta::admin::TapeFileLsItem`] cannot be converted to a [`crate::types::File`]
/// because a required protobuf sub-message is absent.
#[derive(Debug, thiserror::Error)]
#[error("TapeFileLsItem missing field: {0}")]
pub struct TapeFileLsItemConversionError(pub String);
