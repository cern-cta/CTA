// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Recycle bin commands

use crate::typed_admin_cmd;

/// Lists the tape files of the recycle bin (`recycletf ls`).
#[typed_admin_cmd(Recycletapefile::Ls, response = RecycleTapeFileLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the files of this tape
    pub vid: Option<String>,
    /// EOS file ID (hexadecimal)
    pub fxid: Option<String>,
    /// Disk file ID
    pub disk_file_id: Option<String>,
    /// Disk file IDs (decimal)
    #[proto(FileId)]
    pub disk_file_ids: Option<Vec<String>>,
    /// Only this copy number
    #[proto(CopyNumber)]
    pub copy_nb: Option<u64>,
    /// Archive file ID
    pub archive_file_id: Option<u64>,
    /// Disk instance of the disk files
    pub instance: Option<String>,
    /// Only the files recycled at or after this time (seconds since the epoch)
    pub log_unixtime_min: Option<u64>,
    /// Only the files recycled at or before this time (seconds since the epoch)
    pub log_unixtime_max: Option<u64>,
    /// Only the files of this virtual organisation
    pub vo: Option<String>,
}

/// Restores a tape file copy of the recycle bin (`recycletf restore`).
///
/// Not available in `cta-admin`. The admin API requires exactly one of `fxid` and `disk_file_id`, and at least one
/// of `vid` and `archive_file_id`; `instance` and `copy_nb` only narrow the selection. The disk file ID is the one
/// the restored copy is attached to.
#[typed_admin_cmd(Recycletapefile::Restore)]
pub struct Restore {
    /// Tape of the copy to restore
    pub vid: Option<String>,
    /// EOS file ID (hexadecimal) to attach the restored copy to (exclusive with `disk_file_id`)
    pub fxid: Option<String>,
    /// Disk file ID (decimal) to attach the restored copy to (exclusive with `fxid`)
    pub disk_file_id: Option<String>,
    /// Disk instance of the disk file
    pub instance: Option<String>,
    /// Archive file ID of the copy to restore
    pub archive_file_id: Option<u64>,
    /// Copy number of the copy to restore
    #[proto(CopyNumber)]
    pub copy_nb: Option<u64>,
}
