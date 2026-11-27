// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Group mount rule commands

use crate::typed_admin_cmd;

/// Adds a group mount rule (`groupmountrule add`).
#[typed_admin_cmd(Groupmountrule::Add)]
pub struct Add {
    /// Disk instance of the group
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// Name of the group
    #[proto(Username)]
    pub group_name: String,
    /// Mount policy applied to the requests
    #[proto(MountPolicy)]
    pub mount_policy_name: String,
    /// Comment
    pub comment: String,
}

/// Changes a group mount rule (`groupmountrule ch`).
#[typed_admin_cmd(Groupmountrule::Ch)]
pub struct Ch {
    /// Disk instance of the group
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// Name of the group
    #[proto(Username)]
    pub group_name: String,
    /// New mount policy
    #[proto(MountPolicy)]
    pub mount_policy_name: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a group mount rule (`groupmountrule rm`).
#[typed_admin_cmd(Groupmountrule::Rm)]
pub struct Rm {
    /// Disk instance of the group
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// Name of the group
    #[proto(Username)]
    pub group_name: String,
}

/// Lists the group mount rules (`groupmountrule ls`).
#[typed_admin_cmd(Groupmountrule::Ls, response = GroupMountRuleLsItem)]
#[derive(Default)]
pub struct Ls {}
