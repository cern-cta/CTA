// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape file commands

use crate::typed_admin_cmd;

/// Changes the storage class of archive files (`tapefile ch`).
#[typed_admin_cmd(Tapefile::Ch)]
pub struct Ch {
    /// New storage class
    pub storage_class: String,
    /// Disk file IDs (decimal)
    #[proto(FileId)]
    pub disk_file_ids: Option<Vec<String>>,
    /// EOS file ID (hexadecimal)
    pub fxid: Option<String>,
    /// Disk file ID
    pub disk_file_id: Option<String>,
    /// Disk instance of the disk files
    pub instance: Option<String>,
    /// Archive file ID
    pub archive_file_id: Option<u64>,
}

/// Removes a tape file (`tapefile rm`).
#[typed_admin_cmd(Tapefile::Rm)]
pub struct Rm {
    /// Volume identifier of the tape holding the file
    pub vid: String,
    /// Disk instance of the disk file
    pub instance: Option<String>,
    /// EOS file ID (hexadecimal)
    pub fxid: Option<String>,
    /// Disk file ID
    pub disk_file_id: Option<String>,
    /// Archive file ID
    pub archive_file_id: Option<u64>,
    /// Why the tape file is removed
    pub reason: String,
}

/// Lists tape files (`tapefile ls`).
#[typed_admin_cmd(Tapefile::Ls, response = TapeFileLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the files of this tape
    pub vid: Option<String>,
    /// Disk instance of the disk files
    pub instance: Option<String>,
    /// EOS file ID (hexadecimal)
    pub fxid: Option<String>,
    /// Disk file ID
    pub disk_file_id: Option<String>,
    /// Disk file IDs (decimal)
    #[proto(FileId)]
    pub disk_file_ids: Option<Vec<String>>,
    /// Archive file ID
    pub archive_file_id: Option<u64>,
}
