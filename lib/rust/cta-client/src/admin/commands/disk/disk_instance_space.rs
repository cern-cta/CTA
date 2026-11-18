// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instance space commands

use crate::typed_admin_cmd;

/// Adds a disk instance space (`diskinstancespace add`).
#[typed_admin_cmd(Diskinstancespace::Add)]
pub struct Add {
    /// Name of the disk instance space
    #[proto(DiskInstanceSpace)]
    pub name: String,
    /// Disk instance of the space
    pub disk_instance: String,
    /// URL to query the free space of the disk instance
    pub free_space_query_url: String,
    /// How long the result of the free space query is cached, in seconds
    pub refresh_interval: u64,
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
    pub disk_instance: String,
    /// New comment
    pub comment: Option<String>,
    /// New URL of the free space query
    pub free_space_query_url: Option<String>,
    /// New caching time of the free space query, in seconds
    pub refresh_interval: Option<u64>,
}

/// Removes a disk instance space (`diskinstancespace rm`).
#[typed_admin_cmd(Diskinstancespace::Rm)]
pub struct Rm {
    /// Name of the disk instance space
    #[proto(DiskInstanceSpace)]
    pub name: String,
    /// Disk instance of the space
    pub disk_instance: String,
}

/// Lists the disk instance spaces (`diskinstancespace ls`).
#[typed_admin_cmd(Diskinstancespace::Ls, response = DiskInstanceSpaceLsItem)]
#[derive(Default)]
pub struct Ls {}
