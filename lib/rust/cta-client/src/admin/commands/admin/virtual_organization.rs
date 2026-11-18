// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Virtual organisation commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a virtual organisation (`virtualorganization add`).
    #[proto(Virtualorganization.SubcmdAdd)]
    pub struct Add {
        /// Name of the virtual organisation
        vo: str => Vo,
        /// Maximum number of drives the virtual organisation may use for reading
        read_max_drives: u64 => ReadMaxDrives,
        /// Maximum number of drives the virtual organisation may use for writing
        write_max_drives: u64 => WriteMaxDrives,
        /// Comment
        comment: str => Comment,
        /// Disk instance of the virtual organisation
        disk_instance: str => DiskInstance,
        /// Maximum size of the files of the virtual organisation, in bytes
        max_file_size: u64? => MaxFileSize,
        /// Whether the virtual organisation is used for repacking
        is_repack_vo: bool? => IsRepackVo,
    }
}

define_typed_admin_cmd! {
    /// Changes a virtual organisation (`virtualorganization ch`).
    #[proto(Virtualorganization.SubcmdCh)]
    pub struct Ch {
        /// Name of the virtual organisation
        vo: str => Vo,
        /// New comment
        comment: str? => Comment,
        /// New maximum number of drives for reading
        read_max_drives: u64? => ReadMaxDrives,
        /// New maximum number of drives for writing
        write_max_drives: u64? => WriteMaxDrives,
        /// New maximum file size, in bytes
        max_file_size: u64? => MaxFileSize,
        /// New disk instance
        disk_instance: str? => DiskInstance,
        /// Whether the virtual organisation is used for repacking
        is_repack_vo: bool? => IsRepackVo,
    }
}

define_typed_admin_cmd! {
    /// Removes a virtual organisation (`virtualorganization rm`).
    #[proto(Virtualorganization.SubcmdRm)]
    pub struct Rm {
        /// Name of the virtual organisation
        vo: str => Vo,
    }
}

define_typed_admin_cmd! {
    /// Lists the virtual organisations (`virtualorganization ls`).
    #[proto(Virtualorganization.SubcmdLs)]
    #[item(VolsItem: VirtualOrganizationLsItem)]
    #[derive(Default)]
    pub struct Ls {}
}
