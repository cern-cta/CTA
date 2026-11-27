// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instance space commands

use std::time::Duration;

use crate::typed_admin_cmd;

/// Adds a disk instance space (`diskinstancespace add`).
#[typed_admin_cmd(Diskinstancespace::Add)]
pub struct Add {
    /// Name of the disk instance space
    #[proto(DiskInstanceSpace)]
    pub name: String,
    /// Disk instance of the space
    #[proto(DiskInstance)]
    pub disk_instance_name: String,
    /// URL to query the free space of the disk instance
    #[proto(FreeSpaceQueryUrl)]
    pub free_space_url: String,
    /// How long the result of the free space query is cached
    pub refresh_interval: Duration,
    /// Comment
    pub comment: String,
}

/// Changes a disk instance space (`diskinstancespace ch`).
#[typed_admin_cmd(Diskinstancespace::Ch)]
pub struct Ch {
    /// Name of the disk instance space
    #[proto(DiskInstanceSpace)]
    pub name: String,
    /// Disk instance of the space
    #[proto(DiskInstance)]
    pub disk_instance_name: String,
    /// New comment
    pub comment: Option<String>,
    /// New URL of the free space query
    #[proto(FreeSpaceQueryUrl)]
    pub free_space_url: Option<String>,
    /// New caching time of the free space query
    pub refresh_interval: Option<Duration>,
}

/// Removes a disk instance space (`diskinstancespace rm`).
#[typed_admin_cmd(Diskinstancespace::Rm)]
pub struct Rm {
    /// Name of the disk instance space
    #[proto(DiskInstanceSpace)]
    pub name: String,
    /// Disk instance of the space
    #[proto(DiskInstance)]
    pub disk_instance_name: String,
}

/// Lists the disk instance spaces (`diskinstancespace ls`).
#[typed_admin_cmd(Diskinstancespace::Ls, response = DiskInstanceSpaceLsItem)]
#[derive(Default)]
pub struct Ls {}
