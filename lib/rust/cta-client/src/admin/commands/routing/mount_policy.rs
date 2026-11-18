// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Mount policy commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a mount policy (`mountpolicy add`).
    #[proto(Mountpolicy.SubcmdAdd)]
    pub struct Add {
        /// Name of the mount policy
        name: str => MountPolicy,
        /// Priority of archive requests
        archive_priority: u64 => ArchivePriority,
        /// Minimum age of archive requests before mounting, in seconds
        min_archive_request_age: u64 => MinArchiveRequestAge,
        /// Priority of retrieve requests
        retrieve_priority: u64 => RetrievePriority,
        /// Minimum age of retrieve requests before mounting, in seconds
        min_retrieve_request_age: u64 => MinRetrieveRequestAge,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a mount policy (`mountpolicy ch`).
    #[proto(Mountpolicy.SubcmdCh)]
    pub struct Ch {
        /// Name of the mount policy
        name: str => MountPolicy,
        /// New priority of archive requests
        archive_priority: u64? => ArchivePriority,
        /// New minimum age of archive requests, in seconds
        min_archive_request_age: u64? => MinArchiveRequestAge,
        /// New priority of retrieve requests
        retrieve_priority: u64? => RetrievePriority,
        /// New minimum age of retrieve requests, in seconds
        min_retrieve_request_age: u64? => MinRetrieveRequestAge,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a mount policy (`mountpolicy rm`).
    #[proto(Mountpolicy.SubcmdRm)]
    pub struct Rm {
        /// Name of the mount policy
        name: str => MountPolicy,
    }
}

define_typed_admin_cmd! {
    /// Lists the mount policies (`mountpolicy ls`).
    #[proto(Mountpolicy.SubcmdLs)]
    #[item(MplsItem: MountPolicyLsItem)]
    #[derive(Default)]
    pub struct Ls {}
}
