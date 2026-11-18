// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Version command

use crate::typed_admin_cmd;

/// Shows the versions of the client, the frontend and the catalogue (`version`).
#[typed_admin_cmd(Version, response = VersionItem)]
#[derive(Default)]
pub struct Version {}
