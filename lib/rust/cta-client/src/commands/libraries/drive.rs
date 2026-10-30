// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape drive commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Puts drives up, able to mount tapes (`drive up`).
    #[proto(Drive.SubcmdUp)]
    pub struct Up {
        /// Regular expression matching the drive names
        name: str => Drive,
        /// Why the drives are put up
        reason: str? => Reason,
    }
}

define_typed_admin_cmd! {
    /// Puts drives down, unable to mount tapes once their running session is over (`drive down`).
    #[proto(Drive.SubcmdDown)]
    pub struct Down {
        /// Regular expression matching the drive names
        name: str => Drive,
        /// Why the drives are put down, shown by `drive ls`
        reason: str => Reason,
        /// Force the drives down (`--force`)
        force: flag => Force,
    }
}

define_typed_admin_cmd! {
    /// Lists drives (`drive ls`).
    #[proto(Drive.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {
        /// Regular expression matching the drive names; all drives by default
        name: str? => Drive,
        /// All the drives of the catalogue, including those of other scheduler backends
        all: flag => All,
    }
}

define_typed_admin_cmd! {
    /// Removes drive definitions (`drive rm`).
    #[proto(Drive.SubcmdRm)]
    pub struct Rm {
        /// Regular expression matching the drive names
        name: str => Drive,
        /// Remove the drives even if they are not down
        force: flag => Force,
    }
}

define_typed_admin_cmd! {
    /// Changes the comment of drives (`drive ch`).
    #[proto(Drive.SubcmdCh)]
    pub struct Ch {
        /// Regular expression matching the drive names
        name: str => Drive,
        /// New comment
        comment: str => Comment,
    }
}
