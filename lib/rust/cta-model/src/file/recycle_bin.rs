// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The recycle bin of deleted tape files

use cta_protobuf::cta::admin::RecycleTapeFileLsItem;
use jiff::Timestamp;
use serde::Serialize;

use super::archive::checksums;
use crate::{
    ConversionError, Keyed,
    convert::timestamp,
    file::{ArchiveFile, DiskFile, File, Owner, TapeFile},
};

/// A deleted copy of an archived file, kept in the recycle bin of the catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecycleBinTapeFile {
    /// The file, as it was before its deletion
    pub file: File,
    /// When the file was deleted
    pub deletion_time: Timestamp,
    /// Why the file was deleted
    pub deletion_reason: String,
    /// The virtual organisation the file belonged to
    pub virtual_organisation_name: String,
}

impl Keyed for RecycleBinTapeFile {
    type Key<'a> = <File as Keyed>::Key<'a>;

    fn key(&self) -> Self::Key<'_> {
        self.file.key()
    }
}

impl TryFrom<RecycleTapeFileLsItem> for RecycleBinTapeFile {
    type Error = ConversionError;

    fn try_from(val: RecycleTapeFileLsItem) -> Result<Self, Self::Error> {
        Ok(RecycleBinTapeFile {
            file: File {
                archive_file: ArchiveFile {
                    archive_id: val.archive_file_id,
                    storage_class_name: val.storage_class,
                    creation_time: timestamp(
                        "archive_file_creation_time",
                        val.archive_file_creation_time,
                    )?,
                    size: val.size_in_bytes,
                    checksums: checksums("checksum", &val.checksum)?,
                },
                tape_file: TapeFile {
                    vid: val.vid,
                    copy_number: val.copy_nb,
                    block_id: val.block_id,
                    fseq: val.fseq,
                },
                disk_file: DiskFile {
                    disk_instance_name: val.disk_instance,
                    id: val.disk_file_id,
                    id_when_deleted: Some(val.disk_file_id_when_deleted),
                    owner: Some(Owner(
                        u32::try_from(val.disk_file_uid).map_err(|_| {
                            ConversionError::invalid_value("disk_file_uid", val.disk_file_uid)
                        })?,
                        u32::try_from(val.disk_file_gid).map_err(|_| {
                            ConversionError::invalid_value("disk_file_gid", val.disk_file_gid)
                        })?,
                    )),
                    path: val.disk_file_path,
                },
            },
            deletion_time: timestamp("recycle_log_time", val.recycle_log_time)?,
            deletion_reason: val.reason_log,
            virtual_organisation_name: val.virtual_organization,
        })
    }
}
