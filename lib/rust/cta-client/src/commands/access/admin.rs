// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Administrator commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds an administrator (`admin add`).
    #[proto(Admin.SubcmdAdd)]
    pub struct Add {
        /// User name of the administrator
        username: str => Username,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes the comment of an administrator (`admin ch`).
    #[proto(Admin.SubcmdCh)]
    pub struct Ch {
        /// User name of the administrator
        username: str => Username,
        /// New comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes an administrator (`admin rm`).
    #[proto(Admin.SubcmdRm)]
    pub struct Rm {
        /// User name of the administrator
        username: str => Username,
    }
}

define_typed_admin_cmd! {
    /// Lists the administrators (`admin ls`).
    #[proto(Admin.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {}
}
