// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instance space commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a disk instance space (`diskinstancespace add`).
    #[proto(Diskinstancespace.SubcmdAdd)]
    pub struct Add {
        /// Name of the disk instance space
        name: str => DiskInstanceSpace,
        /// Disk instance of the space
        disk_instance: str => DiskInstance,
        /// URL to query the free space of the disk instance
        free_space_query_url: str => FreeSpaceQueryUrl,
        /// How long the result of the free space query is cached, in seconds
        refresh_interval: u64 => RefreshInterval,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a disk instance space (`diskinstancespace ch`).
    #[proto(Diskinstancespace.SubcmdCh)]
    pub struct Ch {
        /// Name of the disk instance space
        name: str => DiskInstanceSpace,
        /// Disk instance of the space
        disk_instance: str => DiskInstance,
        /// New comment
        comment: str? => Comment,
        /// New URL of the free space query
        free_space_query_url: str? => FreeSpaceQueryUrl,
        /// New caching time of the free space query, in seconds
        refresh_interval: u64? => RefreshInterval,
    }
}

define_typed_admin_cmd! {
    /// Removes a disk instance space (`diskinstancespace rm`).
    #[proto(Diskinstancespace.SubcmdRm)]
    pub struct Rm {
        /// Name of the disk instance space
        name: str => DiskInstanceSpace,
        /// Disk instance of the space
        disk_instance: str => DiskInstance,
    }
}

define_typed_admin_cmd! {
    /// Lists the disk instance spaces (`diskinstancespace ls`).
    #[proto(Diskinstancespace.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {}
}
