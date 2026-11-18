// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk system commands

use crate::typed_admin_cmd;

/// Adds a disk system (`disksystem add`).
#[typed_admin_cmd(Disksystem::Add)]
pub struct Add {
    /// Name of the disk system
    pub disk_system: String,
    /// Regular expression matching the destination URLs of the disk system
    pub file_regexp: String,
    /// Disk instance of the disk system
    pub disk_instance: String,
    /// Disk instance space of the disk system
    pub disk_instance_space: String,
    /// Free space to keep before processing requests, in bytes
    pub targeted_free_space: u64,
    /// How long to wait when the free space is insufficient, in seconds
    pub sleep_time: u64,
    /// Comment
    pub comment: String,
}

/// Changes a disk system (`disksystem ch`).
#[typed_admin_cmd(Disksystem::Ch)]
pub struct Ch {
    /// Name of the disk system
    pub disk_system: String,
    /// New regular expression matching the destination URLs
    pub file_regexp: Option<String>,
    /// New free space to keep, in bytes
    pub targeted_free_space: Option<u64>,
    /// New waiting time, in seconds
    pub sleep_time: Option<u64>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a disk system (`disksystem rm`).
#[typed_admin_cmd(Disksystem::Rm)]
pub struct Rm {
    /// Name of the disk system
    pub disk_system: String,
}

/// Lists the disk systems (`disksystem ls`).
#[typed_admin_cmd(Disksystem::Ls, response = DiskSystemLsItem)]
#[derive(Default)]
pub struct Ls {}
