// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Common types used to interface with user code.
//! The idea is to try to hide the ugliness of protobuf code, and provide instead
//! types which are operation-specific

use std::{convert::TryFrom, num::ParseIntError};

use bon::Builder;
use chrono::{DateTime, Utc};
use cta_protobuf::cta::admin::{RecycleTapeFileLsItem, TapeFileLsItem};
use serde::Serialize;
use strum::Display;

use super::errors::Error;

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

impl TryInto<ChecksumType> for cta_protobuf::cta::common::checksum_blob::checksum::Type {
    type Error = ChecksumTypeError<Self>;

    fn try_into(self) -> Result<ChecksumType, Self::Error> {
        Ok(match self {
            Self::None => return Err(ChecksumTypeError::UnknownType(Self::None)),
            Self::Adler32 => ChecksumType::Adler32,
            Self::Crc32 => ChecksumType::Crc32,
            Self::Crc32c => ChecksumType::Crc32c,
            Self::Md5 => ChecksumType::Md5,
            Self::Sha1 => ChecksumType::Sha1,
            Self::Crc64 => ChecksumType::Crc64,
            Self::Sha256 => ChecksumType::Sha256,
            Self::Xxhash64 => ChecksumType::XxHash64,
            Self::Blake3 => ChecksumType::Blake3,
            Self::Hwh64 => ChecksumType::HwH64,
        })
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
    pub fn try_from_eos(text: impl AsRef<str>) -> Result<Self, ChecksumTypeError<()>> {
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
}

impl TryFrom<&cta_protobuf::cta::admin::Checksum> for Checksum {
    type Error = ChecksumTypeError<cta_protobuf::cta::common::checksum_blob::checksum::Type>;

    fn try_from(src: &cta_protobuf::cta::admin::Checksum) -> Result<Self, Self::Error> {
        Ok(Self {
            r#type: src.r#type().try_into()?,
            value: hex_to_byte_array(&src.value)
                .map_err(|e| ChecksumTypeError::Parse(e.to_string()))?,
        })
    }
}

/// Establishes the filtering criteria for a query on the catalogue.
/// Default case (all `None`) means "all elements".
#[derive(Debug, Default, Builder)]
pub struct FileSelector {
    /// Archive ID - ID in CTA catalogue
    #[builder(into)]
    pub archive_file_id: Option<u64>,
    /// Volume ID (ID of tape)
    #[builder(into)]
    pub vid: Option<String>,
    /// Number of copy (normally used together with VID)
    #[builder(into)]
    pub copy_number: Option<u64>,
    /// Disk instance (e.g. EOS instance name)
    #[builder(into)]
    pub disk_instance: Option<String>,
    /// Disk file IDs (normally used together with `disk_instance`)
    #[builder(into)]
    pub file_ids: Option<Vec<String>>,
}

/// Owner (user/group) of a namespace entry.
#[derive(Debug, Clone, Serialize)]
pub struct Owner(/* uid: */ pub u32, /* gid: */ pub u32);

/// A disk file as known to the CTA catalogue.
#[derive(Debug, Clone, Serialize)]
pub struct DiskFile {
    /// The disk storage instance (e.g. EOS instance)
    pub instance: String,
    /// The disk file identifier on the storage system
    pub id: String,
    /// The full file path in the namespace
    pub path: String,
    /// The file identifier at the time it was deleted (if deleted)
    pub id_when_deleted: Option<String>,
    /// The file owner (user/group), if known
    pub owner: Option<Owner>,
}

impl From<cta_protobuf::cta::common::OwnerId> for Owner {
    fn from(value: cta_protobuf::cta::common::OwnerId) -> Self {
        Owner(value.uid, value.gid)
    }
}

impl From<cta_protobuf::cta::admin::tape_file_ls_item::DiskFile> for DiskFile {
    fn from(val: cta_protobuf::cta::admin::tape_file_ls_item::DiskFile) -> Self {
        DiskFile {
            instance: val.disk_instance,
            id: val.disk_id,
            id_when_deleted: None,
            owner: val.owner_id.map(|v| v.into()),
            path: val.path,
        }
    }
}

/// A tape file copy as known to the CTA catalogue.
#[derive(Debug, Clone, Serialize)]
pub struct TapeFile {
    /// Volume ID of the tape on which the file has been written
    pub vid: String,
    /// Copy number
    pub copy_nb: u32,
    /// The position of the file on tape: Logical Block ID
    pub block_id: u64,
    /// The position of the file on tape: File Sequence number
    pub f_seq: u64,
}

impl From<cta_protobuf::cta::admin::tape_file_ls_item::TapeFile> for TapeFile {
    fn from(val: cta_protobuf::cta::admin::tape_file_ls_item::TapeFile) -> Self {
        TapeFile {
            vid: val.vid,
            copy_nb: val.copy_nb,
            block_id: val.block_id,
            f_seq: val.f_seq,
        }
    }
}

/// An archived (tape-resident) file entry.
#[derive(Debug, Clone, Serialize)]
pub struct ArchiveFile {
    /// The archive file identifier
    pub id: u64,
    /// The storage class of the archive file (e.g., `cta_storage_class`)
    pub storage_class: String,
    /// The time at which the file was archived to tape
    pub creation_time: DateTime<Utc>,
    /// The size of the file in bytes
    pub size: u64,
    /// Checksums computed over the file content
    pub checksums: Vec<Checksum>,
}

impl From<cta_protobuf::cta::admin::tape_file_ls_item::ArchiveFile> for ArchiveFile {
    fn from(val: cta_protobuf::cta::admin::tape_file_ls_item::ArchiveFile) -> Self {
        ArchiveFile {
            id: val.archive_id,
            storage_class: val.storage_class,
            creation_time: DateTime::from_timestamp_secs(val.creation_time as i64)
                .expect("Timestamp should be parseable"),
            size: val.size,
            checksums: val
                .checksum
                .iter()
                .map(Checksum::try_from)
                .collect::<Result<Vec<_>, _>>()
                .expect("Checksum should be convertible"),
        }
    }
}

/// A combined file view from the CTA catalogue.
#[derive(Debug, Clone, Serialize)]
pub struct File {
    /// The archive file metadata
    pub archive_file: ArchiveFile,
    /// The tape copy metadata
    pub tape_file: TapeFile,
    /// The disk-side file metadata
    pub disk_file: DiskFile,
    /// The virtual organisation this file belongs to, if any
    pub virtual_org: Option<String>,
}

impl From<RecycleTapeFileLsItem> for File {
    fn from(val: RecycleTapeFileLsItem) -> Self {
        File {
            archive_file: ArchiveFile {
                id: val.archive_file_id,
                storage_class: val.storage_class,
                creation_time: DateTime::from_timestamp_secs(val.archive_file_creation_time as i64)
                    .expect("Timestamp should be valid"),
                size: val.size_in_bytes,
                checksums: val
                    .checksum
                    .iter()
                    .map(Checksum::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("Checksum should be convertible"),
            },
            tape_file: TapeFile {
                vid: val.vid,
                copy_nb: val.copy_nb,
                block_id: val.block_id,
                f_seq: val.fseq,
            },
            disk_file: DiskFile {
                instance: val.disk_instance,
                id: val.disk_file_id,
                id_when_deleted: Some(val.disk_file_id_when_deleted),
                owner: Some(Owner(val.disk_file_uid as u32, val.disk_file_gid as u32)),
                path: val.disk_file_path,
            },
            virtual_org: Some(val.virtual_organization),
        }
    }
}

/// Error returned when a [`TapeFileLsItem`] cannot be converted to a [`File`]
/// because a required protobuf sub-message is absent.
#[derive(Debug, thiserror::Error)]
#[error("TapeFileLsItem missing field: {0}")]
pub struct TapeFileLsItemConversionError(String);

impl TryFrom<TapeFileLsItem> for File {
    type Error = TapeFileLsItemConversionError;

    fn try_from(val: TapeFileLsItem) -> Result<Self, Self::Error> {
        Ok(File {
            archive_file: val
                .af
                .ok_or_else(|| TapeFileLsItemConversionError("archive_file".into()))?
                .into(),
            tape_file: val
                .tf
                .ok_or_else(|| TapeFileLsItemConversionError("tape_file".into()))?
                .into(),
            disk_file: val
                .df
                .ok_or_else(|| TapeFileLsItemConversionError("disk_file".into()))?
                .into(),
            virtual_org: None,
        })
    }
}

impl From<crate::types::TapeFileLsItemConversionError> for Error {
    fn from(e: crate::types::TapeFileLsItemConversionError) -> Self {
        Error::InvalidResponse(e.to_string())
    }
}
