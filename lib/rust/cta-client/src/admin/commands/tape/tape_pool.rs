// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape pool commands

use crate::{admin::commands::CommaSeparated, typed_admin_cmd};

/// Adds a tape pool (`tapepool add`).
#[typed_admin_cmd(Tapepool::Add)]
pub struct Add {
    /// Name of the tape pool
    #[proto(TapePool)]
    pub name: String,
    /// Virtual organisation owning the tape pool
    #[proto(Vo)]
    pub virtual_organisation_name: String,
    /// Number of partially filled tapes to keep for writing
    #[proto(PartialTapesNumber)]
    pub min_writable_tape_count: u64,
    /// Name of the encryption key used for the tapes of the pool
    pub encryption_key_name: Option<String>,
    /// Tape pools supplying this one
    #[builder(into)]
    #[proto(Supply)]
    pub supply_tape_pool_names: Option<CommaSeparated>,
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
    #[proto(Vo)]
    pub virtual_organisation_name: Option<String>,
    /// New number of partially filled tapes
    #[proto(PartialTapesNumber)]
    pub min_writable_tape_count: Option<u64>,
    /// New encryption key name
    pub encryption_key_name: Option<String>,
    /// New list of supplying tape pools
    #[builder(into)]
    #[proto(Supply)]
    pub supply_tape_pool_names: Option<CommaSeparated>,
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
    #[proto(Vo)]
    pub virtual_organisation_name: Option<String>,
    /// Only the encrypted (`true`) or unencrypted (`false`) tape pools
    pub encrypted: Option<bool>,
    /// Only the tape pools using this encryption key
    pub encryption_key_name: Option<String>,
}
