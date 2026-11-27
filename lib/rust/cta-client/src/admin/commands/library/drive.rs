// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape drive commands

use crate::typed_admin_cmd;

/// Puts drives up, able to mount tapes (`drive up`).
#[typed_admin_cmd(Drive::Up)]
pub struct Up {
    /// Regular expression matching the drive names
    #[proto(Drive)]
    pub name: String,
    /// Why the drives are put up
    #[proto(Reason)]
    pub state_reason: Option<String>,
}

/// Puts drives down, unable to mount tapes once their running session is over (`drive down`).
#[typed_admin_cmd(Drive::Down)]
pub struct Down {
    /// Regular expression matching the drive names
    #[proto(Drive)]
    pub name: String,
    /// Why the drives are put down, shown by `drive ls`
    #[proto(Reason)]
    pub state_reason: String,
    /// Force the drives down (`--force`)
    #[proto(only_if_true)]
    pub force: bool,
}

/// Lists drives (`drive ls`).
#[typed_admin_cmd(Drive::Ls, response = DriveLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Regular expression matching the drive names; all drives by default
    #[proto(Drive)]
    pub name: Option<String>,
    /// All the drives of the catalogue, including those of other scheduler backends
    #[proto(only_if_true)]
    pub all: bool,
}

/// Removes drive definitions (`drive rm`).
#[typed_admin_cmd(Drive::Rm)]
pub struct Rm {
    /// Regular expression matching the drive names
    #[proto(Drive)]
    pub name: String,
    /// Remove the drives even if they are not down
    #[proto(only_if_true)]
    pub force: bool,
}

/// Changes the comment of drives (`drive ch`).
#[typed_admin_cmd(Drive::Ch)]
pub struct Ch {
    /// Regular expression matching the drive names
    #[proto(Drive)]
    pub name: String,
    /// New comment
    pub comment: String,
}
