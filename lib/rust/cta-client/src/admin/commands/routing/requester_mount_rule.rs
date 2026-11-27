// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Requester mount rule commands

use crate::typed_admin_cmd;

/// Adds a requester mount rule (`requestermountrule add`).
#[typed_admin_cmd(Requestermountrule::Add)]
pub struct Add {
    /// Disk instance of the requester
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// User name of the requester
    #[proto(Username)]
    pub user_name: String,
    /// Mount policy applied to the requests
    #[proto(MountPolicy)]
    pub mount_policy_name: String,
    /// Comment
    pub comment: String,
}

/// Changes a requester mount rule (`requestermountrule ch`).
#[typed_admin_cmd(Requestermountrule::Ch)]
pub struct Ch {
    /// Disk instance of the requester
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// User name of the requester
    #[proto(Username)]
    pub user_name: String,
    /// New mount policy
    #[proto(MountPolicy)]
    pub mount_policy_name: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a requester mount rule (`requestermountrule rm`).
#[typed_admin_cmd(Requestermountrule::Rm)]
pub struct Rm {
    /// Disk instance of the requester
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// User name of the requester
    #[proto(Username)]
    pub user_name: String,
}

/// Lists the requester mount rules (`requestermountrule ls`).
#[typed_admin_cmd(Requestermountrule::Ls, response = RequesterMountRuleLsItem)]
#[derive(Default)]
pub struct Ls {}
