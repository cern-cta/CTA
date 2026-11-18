// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instance commands

use crate::typed_admin_cmd;

/// Adds a disk instance (`diskinstance add`).
#[typed_admin_cmd(Diskinstance::Add)]
pub struct Add {
    /// Name of the disk instance
    #[proto(DiskInstance)]
    pub name: String,
    /// Comment
    pub comment: String,
}

/// Changes a disk instance (`diskinstance ch`).
#[typed_admin_cmd(Diskinstance::Ch)]
pub struct Ch {
    /// Name of the disk instance
    #[proto(DiskInstance)]
    pub name: String,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a disk instance (`diskinstance rm`).
#[typed_admin_cmd(Diskinstance::Rm)]
pub struct Rm {
    /// Name of the disk instance
    #[proto(DiskInstance)]
    pub name: String,
}

/// Lists the disk instances (`diskinstance ls`).
#[typed_admin_cmd(Diskinstance::Ls, response = DiskInstanceLsItem)]
#[derive(Default)]
pub struct Ls {}
