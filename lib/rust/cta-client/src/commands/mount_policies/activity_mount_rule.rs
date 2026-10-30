// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Activity mount rule commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds an activity mount rule (`activitymountrule add`).
    #[proto(Activitymountrule.SubcmdAdd)]
    pub struct Add {
        /// Disk instance of the requester
        instance: str => Instance,
        /// User name of the requester
        name: str => Username,
        /// Regular expression matching the activity of the requests
        activity_regex: str => ActivityRegex,
        /// Mount policy applied to the requests
        mount_policy: str => MountPolicy,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes an activity mount rule (`activitymountrule ch`).
    #[proto(Activitymountrule.SubcmdCh)]
    pub struct Ch {
        /// Disk instance of the requester
        instance: str => Instance,
        /// User name of the requester
        name: str => Username,
        /// Regular expression matching the activity of the requests
        activity_regex: str => ActivityRegex,
        /// New mount policy
        mount_policy: str? => MountPolicy,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes an activity mount rule (`activitymountrule rm`).
    #[proto(Activitymountrule.SubcmdRm)]
    pub struct Rm {
        /// Disk instance of the requester
        instance: str => Instance,
        /// User name of the requester
        name: str => Username,
        /// Regular expression matching the activity of the requests
        activity_regex: str => ActivityRegex,
    }
}

define_typed_admin_cmd! {
    /// Lists the activity mount rules (`activitymountrule ls`).
    #[proto(Activitymountrule.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {}
}
