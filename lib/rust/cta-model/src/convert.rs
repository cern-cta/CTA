// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The conversion of the protobuf records into the domain types: the error they give, and the helpers they share.

use jiff::Timestamp;

use crate::file::ChecksumAlgorithmError;

/// A record of the frontend API cannot be converted into its domain type (the domain types of this crate).
///
/// The records are protobuf messages, where every field is optional and untyped (timestamps are integers, states are
/// strings, ...). The conversion fails when a record misses a sub-message the domain type needs, or carries a value
/// the domain type cannot represent.
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    /// A sub-message the domain type needs is absent
    #[error("{record} record without `{field}`")]
    MissingField {
        /// The record type (e.g. `TapePoolLsItem`)
        record: &'static str,
        /// The missing field, as named in the protobuf definition
        field: &'static str,
    },
    /// A timestamp (seconds since the Unix epoch) is out of the representable range
    #[error("`{field}`: timestamp {value} is out of range")]
    InvalidTimestamp {
        /// The field, as named in the protobuf definition
        field: &'static str,
        /// The timestamp
        value: u64,
    },
    /// A value has no counterpart in the domain type (unknown state, enum value, ...)
    #[error("`{field}`: unexpected value {value:?}")]
    InvalidValue {
        /// The field, as named in the protobuf definition
        field: &'static str,
        /// The offending value
        value: String,
    },
    /// A field carrying JSON cannot be parsed
    #[error("`{field}`: invalid JSON: {source}")]
    InvalidJson {
        /// The field, as named in the protobuf definition
        field: &'static str,
        /// The parsing error
        #[source]
        source: serde_json::Error,
    },
    /// A checksum cannot be converted
    #[error("`{field}`: {source}")]
    InvalidChecksum {
        /// The field, as named in the protobuf definition
        field: &'static str,
        /// The conversion error
        #[source]
        source: ChecksumAlgorithmError,
    },
}

impl ConversionError {
    /// A [`ConversionError::InvalidValue`] for `field`.
    pub(crate) fn invalid_value(field: &'static str, value: impl ToString) -> Self {
        Self::InvalidValue {
            field,
            value: value.to_string(),
        }
    }
}

/// The [`Timestamp`] of a time in seconds since the Unix epoch, as the records carry them.
pub(crate) fn timestamp(field: &'static str, secs: u64) -> Result<Timestamp, ConversionError> {
    i64::try_from(secs)
        .ok()
        .and_then(|secs| Timestamp::from_second(secs).ok())
        .ok_or(ConversionError::InvalidTimestamp { field, value: secs })
}

/// Same as [`timestamp`], for the fields where `0` stands for "never".
pub(crate) fn optional_timestamp(
    field: &'static str,
    secs: u64,
) -> Result<Option<Timestamp>, ConversionError> {
    if secs == 0 {
        Ok(None)
    } else {
        timestamp(field, secs).map(Some)
    }
}

/// `None` for the string fields where an empty string stands for "none".
pub(crate) fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// The value of a sub-message `field` of a `record`, which must be present.
pub(crate) fn required<T>(
    record: &'static str,
    field: &'static str,
    value: Option<T>,
) -> Result<T, ConversionError> {
    value.ok_or(ConversionError::MissingField { record, field })
}
