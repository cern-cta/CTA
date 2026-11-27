// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Activity mount rule commands

use crate::typed_admin_cmd;

/// Adds an activity mount rule (`activitymountrule add`).
#[typed_admin_cmd(Activitymountrule::Add)]
pub struct Add {
    /// Disk instance of the requester
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// User name of the requester
    #[proto(Username)]
    pub user_name: String,
    /// Regular expression matching the activity of the requests
    #[proto(ActivityRegex)]
    pub regex: String,
    /// Mount policy applied to the requests
    #[proto(MountPolicy)]
    pub mount_policy_name: String,
    /// Comment
    pub comment: String,
}

/// Changes an activity mount rule (`activitymountrule ch`).
#[typed_admin_cmd(Activitymountrule::Ch)]
pub struct Ch {
    /// Disk instance of the requester
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// User name of the requester
    #[proto(Username)]
    pub user_name: String,
    /// Regular expression matching the activity of the requests
    #[proto(ActivityRegex)]
    pub regex: String,
    /// New mount policy
    #[proto(MountPolicy)]
    pub mount_policy_name: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes an activity mount rule (`activitymountrule rm`).
#[typed_admin_cmd(Activitymountrule::Rm)]
pub struct Rm {
    /// Disk instance of the requester
    #[proto(Instance)]
    pub disk_instance_name: String,
    /// User name of the requester
    #[proto(Username)]
    pub user_name: String,
    /// Regular expression matching the activity of the requests
    #[proto(ActivityRegex)]
    pub regex: String,
}

/// Lists the activity mount rules (`activitymountrule ls`).
#[typed_admin_cmd(Activitymountrule::Ls, response = ActivityMountRuleLsItem)]
#[derive(Default)]
pub struct Ls {}
