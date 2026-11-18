// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Administrator commands

use crate::typed_admin_cmd;

/// Adds an administrator (`admin add`).
#[typed_admin_cmd(Admin::Add)]
pub struct Add {
    /// User name of the administrator
    pub username: String,
    /// Comment
    pub comment: String,
}

/// Changes the comment of an administrator (`admin ch`).
#[typed_admin_cmd(Admin::Ch)]
pub struct Ch {
    /// User name of the administrator
    pub username: String,
    /// New comment
    pub comment: String,
}

/// Removes an administrator (`admin rm`).
#[typed_admin_cmd(Admin::Rm)]
pub struct Rm {
    /// User name of the administrator
    pub username: String,
}

/// Lists the administrators (`admin ls`).
#[typed_admin_cmd(Admin::Ls, response = AdminLsItem)]
#[derive(Default)]
pub struct Ls {}
