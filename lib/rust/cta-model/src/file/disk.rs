// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The disk-side metadata of archived files

use cta_protobuf::cta::{admin::tape_file_ls_item, common};
use serde::Serialize;

use crate::file::Owner;

/// A disk file as known to the CTA catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiskFile {
    /// The disk storage instance (e.g. EOS instance)
    pub instance: String,
    /// The disk file identifier on the storage system
    pub id: String,
    /// The full file path in the namespace
    pub path: String,
    /// The file identifier at the time it was deleted (recycle bin entries only)
    pub id_when_deleted: Option<String>,
    /// The file owner (user/group), if known
    pub owner: Option<Owner>,
}

impl From<tape_file_ls_item::DiskFile> for DiskFile {
    fn from(val: tape_file_ls_item::DiskFile) -> Self {
        DiskFile {
            instance: val.disk_instance,
            id: val.disk_id,
            id_when_deleted: None,
            owner: val.owner_id.map(Into::into),
            path: val.path,
        }
    }
}

impl DiskFile {
    /// The disk file of a `cta.common.ArchiveFile`.
    pub(crate) fn from_common(af: &mut common::ArchiveFile) -> Self {
        let df = af.df.take().unwrap_or_default();
        DiskFile {
            instance: std::mem::take(&mut af.disk_instance),
            id: std::mem::take(&mut af.disk_id),
            path: df.path,
            id_when_deleted: None,
            owner: df.owner_id.map(Into::into),
        }
    }
}
