// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Logical library commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a logical library (`logicallibrary add`).
    #[proto(Logicallibrary.SubcmdAdd)]
    pub struct Add {
        /// Name of the logical library
        name: str => LogicalLibrary,
        /// Whether the library is disabled
        disabled: bool? => Disabled,
        /// Physical library the logical library belongs to
        physical_library: str? => PhysicalLibrary,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a logical library (`logicallibrary ch`).
    #[proto(Logicallibrary.SubcmdCh)]
    pub struct Ch {
        /// Name of the logical library
        name: str => LogicalLibrary,
        /// Whether the library is disabled
        disabled: bool? => Disabled,
        /// New physical library
        physical_library: str? => PhysicalLibrary,
        /// New comment
        comment: str? => Comment,
        /// Why the library is disabled
        disabled_reason: str? => DisabledReason,
    }
}

define_typed_admin_cmd! {
    /// Removes a logical library (`logicallibrary rm`).
    #[proto(Logicallibrary.SubcmdRm)]
    pub struct Rm {
        /// Name of the logical library
        name: str => LogicalLibrary,
    }
}

define_typed_admin_cmd! {
    /// Lists the logical libraries (`logicallibrary ls`).
    #[proto(Logicallibrary.SubcmdLs)]
    #[item(LllsItem: LogicalLibraryLsItem)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the disabled (`true`) or enabled (`false`) libraries
        disabled: bool? => Disabled,
    }
}
