// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Virtual organisation commands

use crate::typed_admin_cmd;

/// Adds a virtual organisation (`virtualorganization add`).
#[typed_admin_cmd(Virtualorganization::Add)]
pub struct Add {
    /// Name of the virtual organisation
    pub vo: String,
    /// Maximum number of drives the virtual organisation may use for reading
    pub read_max_drives: u64,
    /// Maximum number of drives the virtual organisation may use for writing
    pub write_max_drives: u64,
    /// Comment
    pub comment: String,
    /// Disk instance of the virtual organisation
    pub disk_instance: String,
    /// Maximum size of the files of the virtual organisation, in bytes
    pub max_file_size: Option<u64>,
    /// Whether the virtual organisation is used for repacking
    pub is_repack_vo: Option<bool>,
}

/// Changes a virtual organisation (`virtualorganization ch`).
#[typed_admin_cmd(Virtualorganization::Ch)]
pub struct Ch {
    /// Name of the virtual organisation
    pub vo: String,
    /// New comment
    pub comment: Option<String>,
    /// New maximum number of drives for reading
    pub read_max_drives: Option<u64>,
    /// New maximum number of drives for writing
    pub write_max_drives: Option<u64>,
    /// New maximum file size, in bytes
    pub max_file_size: Option<u64>,
    /// New disk instance
    pub disk_instance: Option<String>,
    /// Whether the virtual organisation is used for repacking
    pub is_repack_vo: Option<bool>,
}

/// Removes a virtual organisation (`virtualorganization rm`).
#[typed_admin_cmd(Virtualorganization::Rm)]
pub struct Rm {
    /// Name of the virtual organisation
    pub vo: String,
}

/// Lists the virtual organisations (`virtualorganization ls`).
#[typed_admin_cmd(Virtualorganization::Ls, response = VirtualOrganizationLsItem)]
#[derive(Default)]
pub struct Ls {}
