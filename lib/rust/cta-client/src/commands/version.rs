// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Version command

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Shows the versions of the client, the frontend and the catalogue (`version`).
    #[proto(Version.SubcmdNone)]
    #[item(VersionItem: VersionItem)]
    #[derive(Default)]
    pub struct Version {}
}
