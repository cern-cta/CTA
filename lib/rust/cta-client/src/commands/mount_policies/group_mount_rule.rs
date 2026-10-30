// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Group mount rule commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a group mount rule (`groupmountrule add`).
    #[proto(Groupmountrule.SubcmdAdd)]
    pub struct Add {
        /// Disk instance of the group
        instance: str => Instance,
        /// Name of the group
        name: str => Username,
        /// Mount policy applied to the requests
        mount_policy: str => MountPolicy,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a group mount rule (`groupmountrule ch`).
    #[proto(Groupmountrule.SubcmdCh)]
    pub struct Ch {
        /// Disk instance of the group
        instance: str => Instance,
        /// Name of the group
        name: str => Username,
        /// New mount policy
        mount_policy: str? => MountPolicy,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a group mount rule (`groupmountrule rm`).
    #[proto(Groupmountrule.SubcmdRm)]
    pub struct Rm {
        /// Disk instance of the group
        instance: str => Instance,
        /// Name of the group
        name: str => Username,
    }
}

define_typed_admin_cmd! {
    /// Lists the group mount rules (`groupmountrule ls`).
    #[proto(Groupmountrule.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {}
}
