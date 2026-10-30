// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape file commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Changes the storage class of archive files (`tapefile ch`).
    #[proto(Tapefile.SubcmdCh)]
    pub struct Ch {
        /// New storage class
        storage_class: str => StorageClass,
        /// Disk file IDs (decimal)
        disk_file_ids: str_list? => FileId,
        /// EOS file ID (hexadecimal)
        fxid: str? => Fxid,
        /// Disk file ID
        disk_file_id: str? => DiskFileId,
        /// Disk instance of the disk files
        instance: str? => Instance,
        /// Archive file ID
        archive_file_id: u64? => ArchiveFileId,
    }
}

define_typed_admin_cmd! {
    /// Removes a tape file (`tapefile rm`).
    #[proto(Tapefile.SubcmdRm)]
    pub struct Rm {
        /// Volume identifier of the tape holding the file
        vid: str => Vid,
        /// Disk instance of the disk file
        instance: str? => Instance,
        /// EOS file ID (hexadecimal)
        fxid: str? => Fxid,
        /// Disk file ID
        disk_file_id: str? => DiskFileId,
        /// Archive file ID
        archive_file_id: u64? => ArchiveFileId,
        /// Why the tape file is removed
        reason: str => Reason,
    }
}

define_typed_admin_cmd! {
    /// Lists tape files (`tapefile ls`).
    #[proto(Tapefile.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the files of this tape
        vid: str? => Vid,
        /// Disk instance of the disk files
        instance: str? => Instance,
        /// EOS file ID (hexadecimal)
        fxid: str? => Fxid,
        /// Disk file ID
        disk_file_id: str? => DiskFileId,
        /// Disk file IDs (decimal)
        disk_file_ids: str_list? => FileId,
        /// Archive file ID
        archive_file_id: u64? => ArchiveFileId,
    }
}
