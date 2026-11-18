// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Archived files: their copies on tape, their disk-side metadata and checksums, and the recycle bin of the deleted
//! copies

use std::fmt;

use cta_protobuf::cta::{admin::TapeFileLsItem, common};
use serde::Serialize;

use crate::{ConversionError, Keyed, convert::required};

mod archive;
mod checksum;
mod disk;
mod recycle_bin;
mod tape;
#[cfg(test)]
mod tests;

pub use archive::ArchiveFile;
pub use checksum::{Checksum, ChecksumType, ChecksumTypeError, HexDecodeError, hex_to_byte_array};
pub use disk::DiskFile;
pub use recycle_bin::RecycleBinTapeFile;
pub use tape::TapeFile;

/// Owner (user/group) of a namespace entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct Owner(/* uid: */ pub u32, /* gid: */ pub u32);

impl From<common::OwnerId> for Owner {
    fn from(value: common::OwnerId) -> Self {
        Owner(value.uid, value.gid)
    }
}

/// A copy of an archived file on tape, with the disk-side metadata of the file.
///
/// Identified by its archive file, tape and position on the tape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct File {
    /// The archive file metadata
    pub archive_file: ArchiveFile,
    /// The tape copy metadata
    pub tape_file: TapeFile,
    /// The disk-side file metadata
    pub disk_file: DiskFile,
}

impl Keyed for File {
    type Key<'a> = (u64, &'a str, u64);

    fn key(&self) -> Self::Key<'_> {
        (
            self.archive_file.id,
            &self.tape_file.vid,
            self.tape_file.f_seq,
        )
    }
}

impl TryFrom<TapeFileLsItem> for File {
    type Error = ConversionError;

    fn try_from(val: TapeFileLsItem) -> Result<Self, Self::Error> {
        const RECORD: &str = "TapeFileLsItem";
        Ok(File {
            archive_file: required(RECORD, "af", val.af)?.try_into()?,
            tape_file: required(RECORD, "tf", val.tf)?.into(),
            disk_file: required(RECORD, "df", val.df)?.into(),
        })
    }
}

/// How to designate an archived file: by its archive file ID, or by its disk file ID on a disk instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FileId {
    /// The archive file ID
    Archive(u64),
    /// The disk file ID on a disk instance
    Disk {
        /// The disk file ID
        disk_file_id: String,
        /// The disk instance
        disk_instance: String,
    },
}

impl FileId {
    /// A file designated by its disk file ID on a disk instance.
    pub fn disk(disk_file_id: impl Into<String>, disk_instance: impl Into<String>) -> Self {
        Self::Disk {
            disk_file_id: disk_file_id.into(),
            disk_instance: disk_instance.into(),
        }
    }

    /// The archive file ID, for [`FileId::Archive`].
    #[must_use]
    pub fn archive_file_id(&self) -> Option<u64> {
        match self {
            Self::Archive(id) => Some(*id),
            Self::Disk { .. } => None,
        }
    }

    /// The disk file ID, for [`FileId::Disk`].
    #[must_use]
    pub fn disk_file_id(&self) -> Option<String> {
        match self {
            Self::Archive(_) => None,
            Self::Disk { disk_file_id, .. } => Some(disk_file_id.clone()),
        }
    }

    /// The disk instance, for [`FileId::Disk`].
    #[must_use]
    pub fn disk_instance(&self) -> Option<String> {
        match self {
            Self::Archive(_) => None,
            Self::Disk { disk_instance, .. } => Some(disk_instance.clone()),
        }
    }
}

impl From<u64> for FileId {
    fn from(archive_file_id: u64) -> Self {
        Self::Archive(archive_file_id)
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Archive(id) => write!(f, "{id}"),
            Self::Disk {
                disk_file_id,
                disk_instance,
            } => write!(f, "{disk_file_id}@{disk_instance}"),
        }
    }
}
