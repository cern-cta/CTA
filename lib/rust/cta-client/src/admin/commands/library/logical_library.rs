// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Logical library commands

use crate::typed_admin_cmd;

/// Adds a logical library (`logicallibrary add`).
#[typed_admin_cmd(Logicallibrary::Add)]
pub struct Add {
    /// Name of the logical library
    #[proto(LogicalLibrary)]
    pub name: String,
    /// Whether the library is disabled
    pub disabled: Option<bool>,
    /// Physical library the logical library belongs to
    #[proto(PhysicalLibrary)]
    pub physical_library_name: Option<String>,
    /// Comment
    pub comment: String,
}

/// Changes a logical library (`logicallibrary ch`).
#[typed_admin_cmd(Logicallibrary::Ch)]
pub struct Ch {
    /// Name of the logical library
    #[proto(LogicalLibrary)]
    pub name: String,
    /// Whether the library is disabled
    pub disabled: Option<bool>,
    /// New physical library
    #[proto(PhysicalLibrary)]
    pub physical_library_name: Option<String>,
    /// New comment
    pub comment: Option<String>,
    /// Why the library is disabled
    #[proto(DisabledReason)]
    pub state_reason: Option<String>,
}

/// Removes a logical library (`logicallibrary rm`).
#[typed_admin_cmd(Logicallibrary::Rm)]
pub struct Rm {
    /// Name of the logical library
    #[proto(LogicalLibrary)]
    pub name: String,
}

/// Lists the logical libraries (`logicallibrary ls`).
#[typed_admin_cmd(Logicallibrary::Ls, response = LogicalLibraryLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the disabled (`true`) or enabled (`false`) libraries
    pub disabled: Option<bool>,
}
