// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Checksums of the archived files

use std::num::ParseIntError;

use cta_protobuf::cta::common::checksum_blob::checksum::Type as ProtoChecksumType;
use serde::Serialize;
use strum::Display;

/// An error type for the conversion of checksum types. Useful to convert between EOS and CTA.
#[derive(Debug, thiserror::Error)]
pub enum ChecksumTypeError {
    /// Error parsing a checksum name string
    #[error("Error parsing string '{0}'")]
    Parse(String),
    /// Error converting from a source checksum type, named as in its source (e.g. `NONE` for the protocol)
    #[error("Source checksum value not supported: {0}")]
    UnknownType(String),
}

/// An error returned by [`hex_to_byte_array`] when the input string cannot be decoded.
#[derive(Debug, thiserror::Error)]
pub enum HexDecodeError {
    /// A non-hexadecimal character was found in the input string.
    #[error("non-hexadecimal character: '{0}'")]
    NonHexCharacter(char),
    /// An integer parsing error from `std`.
    #[error("hexadecimal parsing error: {0}")]
    ParseInt(#[from] ParseIntError),
}

/// Decodes a hexadecimal string into its bytes, most significant byte first.
///
/// An optional `0x`/`0X` prefix is accepted and an odd number of digits is
/// treated as if it were left-padded with a zero, so `"0x1a2"` decodes to
/// `[0x01, 0xa2]`. An empty string (or a bare prefix) decodes to an empty
/// vector.
///
/// # Errors
///
/// Returns a [`HexDecodeError`] if the input contains anything other than
/// hexadecimal digits, including non-ASCII characters.
pub fn hex_to_byte_array(hex_string: &str) -> Result<Vec<u8>, HexDecodeError> {
    let digits = hex_string
        .strip_prefix("0x")
        .or_else(|| hex_string.strip_prefix("0X"))
        .unwrap_or(hex_string);

    // Reject anything that is not an ASCII hex digit up front
    if let Some(invalid) = digits.chars().find(|c| !c.is_ascii_hexdigit()) {
        return Err(HexDecodeError::NonHexCharacter(invalid));
    }

    // From here on every character is one ASCII byte, so byte offsets and
    // character offsets coincide.
    let mut bytes = Vec::with_capacity(digits.len().div_ceil(2));
    let mut offset = 0;

    // An odd number of digits means the leading nibble stands alone.
    if digits.len() % 2 == 1 {
        bytes.push(u8::from_str_radix(&digits[..1], 16)?);
        offset = 1;
    }

    while offset < digits.len() {
        bytes.push(u8::from_str_radix(&digits[offset..offset + 2], 16)?);
        offset += 2;
    }

    Ok(bytes)
}

impl TryFrom<ProtoChecksumType> for ChecksumType {
    type Error = ChecksumTypeError;

    fn try_from(value: ProtoChecksumType) -> Result<Self, Self::Error> {
        Ok(match value {
            ProtoChecksumType::None => {
                return Err(ChecksumTypeError::UnknownType(
                    ProtoChecksumType::None.as_str_name().to_owned(),
                ));
            }
            ProtoChecksumType::Adler32 => Self::Adler32,
            ProtoChecksumType::Crc32 => Self::Crc32,
            ProtoChecksumType::Crc32c => Self::Crc32c,
            ProtoChecksumType::Md5 => Self::Md5,
            ProtoChecksumType::Sha1 => Self::Sha1,
            ProtoChecksumType::Crc64 => Self::Crc64,
            ProtoChecksumType::Sha256 => Self::Sha256,
            ProtoChecksumType::Xxhash64 => Self::XxHash64,
            ProtoChecksumType::Blake3 => Self::Blake3,
            ProtoChecksumType::Hwh64 => Self::HwH64,
        })
    }
}

impl From<ChecksumType> for ProtoChecksumType {
    fn from(value: ChecksumType) -> Self {
        match value {
            ChecksumType::Adler32 => Self::Adler32,
            ChecksumType::Crc32 => Self::Crc32,
            ChecksumType::Crc32c => Self::Crc32c,
            ChecksumType::Md5 => Self::Md5,
            ChecksumType::Sha1 => Self::Sha1,
            ChecksumType::Crc64 => Self::Crc64,
            ChecksumType::Sha256 => Self::Sha256,
            ChecksumType::XxHash64 => Self::Xxhash64,
            ChecksumType::Blake3 => Self::Blake3,
            ChecksumType::HwH64 => Self::Hwh64,
        }
    }
}

/// Types of checksums
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize)]
#[non_exhaustive]
pub enum ChecksumType {
    /// `ADLER 32`
    Adler32,
    /// `CRC-32`
    Crc32,
    /// `CRC32C`
    Crc32c,
    /// `MD5`
    Md5,
    /// `SHA-1`
    Sha1,
    /// `CRC-64`
    Crc64,
    /// `SHA-256`
    Sha256,
    /// `xxHash64`
    XxHash64,
    /// `BLAKE3`
    Blake3,
    /// `HighwayHash64`
    HwH64,
}

impl ChecksumType {
    /// Convert a string to a [`ChecksumType`], according to EOS convention
    ///
    /// # Errors
    /// Unknown hash function names will result in a [`ChecksumTypeError`]
    pub fn try_from_eos(text: impl AsRef<str>) -> Result<Self, ChecksumTypeError> {
        Ok(match text.as_ref() {
            "adler" | "adler32" => Self::Adler32,
            "blake3" => Self::Blake3,
            "crc32" => Self::Crc32,
            "crc32c" => Self::Crc32c,
            "md5" => Self::Md5,
            "sha" => Self::Sha1,
            "sha256" => Self::Sha256,
            "crc64" => Self::Crc64,
            "xxhash64" => Self::XxHash64,
            "hwh64" => Self::HwH64,
            s => return Err(ChecksumTypeError::Parse(s.into())),
        })
    }
}

/// An enum-based, unambiguous, representation of a checksum
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct Checksum {
    /// The type of the checksum
    pub r#type: ChecksumType,
    /// The content, as a vector of bytes
    pub value: Vec<u8>,
}

impl Checksum {
    /// Create a checksum from its components
    pub fn new(r#type: ChecksumType, value: Vec<u8>) -> Self {
        Self { r#type, value }
    }

    /// The value as a lowercase hexadecimal string, as `cta-admin` prints it.
    #[must_use]
    pub fn hex_value(&self) -> String {
        self.value.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// The checksums of the `tapefile ls` and `recycletf ls` records carry their value as a hexadecimal string.
impl TryFrom<&cta_protobuf::cta::admin::Checksum> for Checksum {
    type Error = ChecksumTypeError;

    fn try_from(src: &cta_protobuf::cta::admin::Checksum) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: src.r#type().try_into()?,
            value: hex_to_byte_array(&src.value)
                .map_err(|e| ChecksumTypeError::Parse(e.to_string()))?,
        })
    }
}

/// The checksums of the checksum blobs (`cta.common.ChecksumBlob`) carry their value as bytes.
impl TryFrom<&cta_protobuf::cta::common::checksum_blob::Checksum> for Checksum {
    type Error = ChecksumTypeError;

    fn try_from(
        src: &cta_protobuf::cta::common::checksum_blob::Checksum,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: src.r#type().try_into()?,
            value: src.value.clone(),
        })
    }
}
