// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Archived files

use cta_protobuf::cta::{admin::tape_file_ls_item, common};
use jiff::Timestamp;
use serde::Serialize;

use crate::{ConversionError, convert::timestamp, file::Checksum};

/// An archived (tape-resident) file entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveFile {
    /// The archive file identifier
    pub id: u64,
    /// The storage class of the archive file (e.g., `cta_storage_class`)
    pub storage_class: String,
    /// The time at which the file was archived to tape
    pub creation_time: Timestamp,
    /// The size of the file in bytes
    pub size: u64,
    /// Checksums computed over the file content
    pub checksums: Vec<Checksum>,
}

/// Converts the checksums of a record, `field` naming them in the error.
pub(super) fn checksums<'a, T: 'a>(
    field: &'static str,
    items: impl IntoIterator<Item = &'a T>,
) -> Result<Vec<Checksum>, ConversionError>
where
    Checksum: TryFrom<&'a T, Error = crate::file::ChecksumTypeError>,
{
    items
        .into_iter()
        .map(Checksum::try_from)
        .collect::<Result<_, _>>()
        .map_err(|source| ConversionError::InvalidChecksum { field, source })
}

impl TryFrom<tape_file_ls_item::ArchiveFile> for ArchiveFile {
    type Error = ConversionError;

    fn try_from(val: tape_file_ls_item::ArchiveFile) -> Result<Self, Self::Error> {
        Ok(ArchiveFile {
            id: val.archive_id,
            storage_class: val.storage_class,
            creation_time: timestamp("af.creation_time", val.creation_time)?,
            size: val.size,
            checksums: checksums("af.checksum", &val.checksum)?,
        })
    }
}

impl ArchiveFile {
    /// The archive file of a `cta.common.ArchiveFile`, whose checksums are a blob.
    pub(crate) fn from_common(af: common::ArchiveFile) -> Result<Self, ConversionError> {
        Ok(ArchiveFile {
            id: af.archive_id,
            storage_class: af.storage_class,
            creation_time: timestamp("af.creation_time", af.creation_time)?,
            size: af.size,
            checksums: checksums("af.csb", af.csb.as_ref().map_or(&[][..], |csb| &csb.cs))?,
        })
    }
}
