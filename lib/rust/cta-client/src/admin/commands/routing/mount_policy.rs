// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Mount policy commands

use std::time::Duration;

use crate::typed_admin_cmd;

/// Adds a mount policy (`mountpolicy add`).
#[typed_admin_cmd(Mountpolicy::Add)]
pub struct Add {
    /// Name of the mount policy
    #[proto(MountPolicy)]
    pub name: String,
    /// Priority of archive requests
    pub archive_priority: u64,
    /// Minimum age of archive requests before mounting
    #[proto(MinArchiveRequestAge)]
    pub min_archive_age: Duration,
    /// Priority of retrieve requests
    pub retrieve_priority: u64,
    /// Minimum age of retrieve requests before mounting
    #[proto(MinRetrieveRequestAge)]
    pub min_retrieve_age: Duration,
    /// Comment
    pub comment: String,
}

/// Changes a mount policy (`mountpolicy ch`).
#[typed_admin_cmd(Mountpolicy::Ch)]
pub struct Ch {
    /// Name of the mount policy
    #[proto(MountPolicy)]
    pub name: String,
    /// New priority of archive requests
    pub archive_priority: Option<u64>,
    /// New minimum age of archive requests
    #[proto(MinArchiveRequestAge)]
    pub min_archive_age: Option<Duration>,
    /// New priority of retrieve requests
    pub retrieve_priority: Option<u64>,
    /// New minimum age of retrieve requests
    #[proto(MinRetrieveRequestAge)]
    pub min_retrieve_age: Option<Duration>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a mount policy (`mountpolicy rm`).
#[typed_admin_cmd(Mountpolicy::Rm)]
pub struct Rm {
    /// Name of the mount policy
    #[proto(MountPolicy)]
    pub name: String,
}

/// Lists the mount policies (`mountpolicy ls`).
#[typed_admin_cmd(Mountpolicy::Ls, response = MountPolicyLsItem)]
#[derive(Default)]
pub struct Ls {}
