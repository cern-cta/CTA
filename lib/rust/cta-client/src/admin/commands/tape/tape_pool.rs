// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape pool commands

use crate::typed_admin_cmd;

/// Adds a tape pool (`tapepool add`).
#[typed_admin_cmd(Tapepool::Add)]
pub struct Add {
    /// Name of the tape pool
    #[proto(TapePool)]
    pub name: String,
    /// Virtual organisation owning the tape pool
    pub vo: String,
    /// Number of partially filled tapes to keep for writing
    #[proto(PartialTapesNumber)]
    pub partial_tapes: u64,
    /// Name of the encryption key used for the tapes of the pool
    pub encryption_key_name: Option<String>,
    /// Comma-separated list of the tape pools supplying this one
    pub supply: Option<String>,
    /// Comment
    pub comment: String,
}

/// Changes a tape pool (`tapepool ch`).
#[typed_admin_cmd(Tapepool::Ch)]
pub struct Ch {
    /// Name of the tape pool
    #[proto(TapePool)]
    pub name: String,
    /// New virtual organisation
    pub vo: Option<String>,
    /// New number of partially filled tapes
    #[proto(PartialTapesNumber)]
    pub partial_tapes: Option<u64>,
    /// New encryption key name
    pub encryption_key_name: Option<String>,
    /// New list of supplying tape pools
    pub supply: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a tape pool (`tapepool rm`).
#[typed_admin_cmd(Tapepool::Rm)]
pub struct Rm {
    /// Name of the tape pool
    #[proto(TapePool)]
    pub name: String,
}

/// Lists tape pools (`tapepool ls`); without filters, lists all of them.
#[typed_admin_cmd(Tapepool::Ls, response = TapePoolLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the tape pool with this name
    #[proto(TapePool)]
    pub name: Option<String>,
    /// Only the tape pools of this virtual organisation
    pub vo: Option<String>,
    /// Only the encrypted (`true`) or unencrypted (`false`) tape pools
    pub encrypted: Option<bool>,
    /// Only the tape pools using this encryption key
    pub encryption_key_name: Option<String>,
}
