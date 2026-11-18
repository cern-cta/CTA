// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Recycle bin commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Lists the tape files of the recycle bin (`recycletf ls`).
    #[proto(Recycletapefile.SubcmdLs)]
    #[item(RtflsItem: RecycleTapeFileLsItem)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the files of this tape
        vid: str? => Vid,
        /// EOS file ID (hexadecimal)
        fxid: str? => Fxid,
        /// Disk file ID
        disk_file_id: str? => DiskFileId,
        /// Disk file IDs (decimal)
        disk_file_ids: str_list? => FileId,
        /// Only this copy number
        copy_nb: u64? => CopyNumber,
        /// Archive file ID
        archive_file_id: u64? => ArchiveFileId,
        /// Disk instance of the disk files
        instance: str? => Instance,
        /// Only the files recycled at or after this time (seconds since the epoch)
        log_unixtime_min: u64? => LogUnixtimeMin,
        /// Only the files recycled at or before this time (seconds since the epoch)
        log_unixtime_max: u64? => LogUnixtimeMax,
        /// Only the files of this virtual organisation
        vo: str? => Vo,
    }
}

define_typed_admin_cmd! {
    /// Restores a tape file copy of the recycle bin (`recycletf restore`).
    ///
    /// Not available in `cta-admin`. The admin API requires exactly one of `fxid` and `disk_file_id`, and at least one
    /// of `vid` and `archive_file_id`; `instance` and `copy_nb` only narrow the selection. The disk file ID is the one
    /// the restored copy is attached to.
    #[proto(Recycletapefile.SubcmdRestore)]
    pub struct Restore {
        /// Tape of the copy to restore
        vid: str? => Vid,
        /// EOS file ID (hexadecimal) to attach the restored copy to (exclusive with `disk_file_id`)
        fxid: str? => Fxid,
        /// Disk file ID (decimal) to attach the restored copy to (exclusive with `fxid`)
        disk_file_id: str? => DiskFileId,
        /// Disk instance of the disk file
        instance: str? => Instance,
        /// Archive file ID of the copy to restore
        archive_file_id: u64? => ArchiveFileId,
        /// Copy number of the copy to restore
        copy_nb: u64? => CopyNumber,
    }
}
