// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instance commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a disk instance (`diskinstance add`).
    #[proto(Diskinstance.SubcmdAdd)]
    pub struct Add {
        /// Name of the disk instance
        name: str => DiskInstance,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a disk instance (`diskinstance ch`).
    #[proto(Diskinstance.SubcmdCh)]
    pub struct Ch {
        /// Name of the disk instance
        name: str => DiskInstance,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a disk instance (`diskinstance rm`).
    #[proto(Diskinstance.SubcmdRm)]
    pub struct Rm {
        /// Name of the disk instance
        name: str => DiskInstance,
    }
}

define_typed_admin_cmd! {
    /// Lists the disk instances (`diskinstance ls`).
    #[proto(Diskinstance.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {}
}
