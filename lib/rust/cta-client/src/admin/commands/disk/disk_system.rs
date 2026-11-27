// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk system commands

use std::time::Duration;

use crate::typed_admin_cmd;

/// Adds a disk system (`disksystem add`).
#[typed_admin_cmd(Disksystem::Add)]
pub struct Add {
    /// Name of the disk system
    #[proto(DiskSystem)]
    pub name: String,
    /// Regular expression matching the destination URLs of the disk system
    #[proto(FileRegexp)]
    pub file_regex: String,
    /// Disk instance of the disk system
    #[proto(DiskInstance)]
    pub disk_instance_name: String,
    /// Disk instance space of the disk system
    #[proto(DiskInstanceSpace)]
    pub disk_instance_space_name: String,
    /// Free space to keep before processing requests, in bytes
    #[proto(TargetedFreeSpace)]
    pub desired_min_free_space: u64,
    /// How long to wait when the free space is insufficient
    pub sleep_time: Duration,
    /// Comment
    pub comment: String,
}

/// Changes a disk system (`disksystem ch`).
#[typed_admin_cmd(Disksystem::Ch)]
pub struct Ch {
    /// Name of the disk system
    #[proto(DiskSystem)]
    pub name: String,
    /// New regular expression matching the destination URLs
    #[proto(FileRegexp)]
    pub file_regex: Option<String>,
    /// New free space to keep, in bytes
    #[proto(TargetedFreeSpace)]
    pub desired_min_free_space: Option<u64>,
    /// New waiting time
    pub sleep_time: Option<Duration>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a disk system (`disksystem rm`).
#[typed_admin_cmd(Disksystem::Rm)]
pub struct Rm {
    /// Name of the disk system
    #[proto(DiskSystem)]
    pub name: String,
}

/// Lists the disk systems (`disksystem ls`).
#[typed_admin_cmd(Disksystem::Ls, response = DiskSystemLsItem)]
#[derive(Default)]
pub struct Ls {}
