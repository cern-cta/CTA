// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Requester mount rule commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a requester mount rule (`requestermountrule add`).
    #[proto(Requestermountrule.SubcmdAdd)]
    pub struct Add {
        /// Disk instance of the requester
        instance: str => Instance,
        /// User name of the requester
        name: str => Username,
        /// Mount policy applied to the requests
        mount_policy: str => MountPolicy,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a requester mount rule (`requestermountrule ch`).
    #[proto(Requestermountrule.SubcmdCh)]
    pub struct Ch {
        /// Disk instance of the requester
        instance: str => Instance,
        /// User name of the requester
        name: str => Username,
        /// New mount policy
        mount_policy: str? => MountPolicy,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a requester mount rule (`requestermountrule rm`).
    #[proto(Requestermountrule.SubcmdRm)]
    pub struct Rm {
        /// Disk instance of the requester
        instance: str => Instance,
        /// User name of the requester
        name: str => Username,
    }
}

define_typed_admin_cmd! {
    /// Lists the requester mount rules (`requestermountrule ls`).
    #[proto(Requestermountrule.SubcmdLs)]
    #[item(RmrlsItem: RequesterMountRuleLsItem)]
    #[derive(Default)]
    pub struct Ls {}
}
