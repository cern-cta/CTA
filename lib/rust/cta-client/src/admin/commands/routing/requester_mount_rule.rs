// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Requester mount rule commands

use crate::typed_admin_cmd;

/// Adds a requester mount rule (`requestermountrule add`).
#[typed_admin_cmd(Requestermountrule::Add)]
pub struct Add {
    /// Disk instance of the requester
    pub instance: String,
    /// User name of the requester
    #[proto(Username)]
    pub name: String,
    /// Mount policy applied to the requests
    pub mount_policy: String,
    /// Comment
    pub comment: String,
}

/// Changes a requester mount rule (`requestermountrule ch`).
#[typed_admin_cmd(Requestermountrule::Ch)]
pub struct Ch {
    /// Disk instance of the requester
    pub instance: String,
    /// User name of the requester
    #[proto(Username)]
    pub name: String,
    /// New mount policy
    pub mount_policy: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a requester mount rule (`requestermountrule rm`).
#[typed_admin_cmd(Requestermountrule::Rm)]
pub struct Rm {
    /// Disk instance of the requester
    pub instance: String,
    /// User name of the requester
    #[proto(Username)]
    pub name: String,
}

/// Lists the requester mount rules (`requestermountrule ls`).
#[typed_admin_cmd(Requestermountrule::Ls, response = RequesterMountRuleLsItem)]
#[derive(Default)]
pub struct Ls {}
