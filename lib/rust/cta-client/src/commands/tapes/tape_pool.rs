// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape pool commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a tape pool (`tapepool add`).
    #[proto(Tapepool.SubcmdAdd)]
    pub struct Add {
        /// Name of the tape pool
        name: str => TapePool,
        /// Virtual organisation owning the tape pool
        vo: str => Vo,
        /// Number of partially filled tapes to keep for writing
        partial_tapes: u64 => PartialTapesNumber,
        /// Name of the encryption key used for the tapes of the pool
        encryption_key_name: str? => EncryptionKeyName,
        /// Comma-separated list of the tape pools supplying this one
        supply: str? => Supply,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a tape pool (`tapepool ch`).
    #[proto(Tapepool.SubcmdCh)]
    pub struct Ch {
        /// Name of the tape pool
        name: str => TapePool,
        /// New virtual organisation
        vo: str? => Vo,
        /// New number of partially filled tapes
        partial_tapes: u64? => PartialTapesNumber,
        /// New encryption key name
        encryption_key_name: str? => EncryptionKeyName,
        /// New list of supplying tape pools
        supply: str? => Supply,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a tape pool (`tapepool rm`).
    #[proto(Tapepool.SubcmdRm)]
    pub struct Rm {
        /// Name of the tape pool
        name: str => TapePool,
    }
}

define_typed_admin_cmd! {
    /// Lists tape pools (`tapepool ls`); without filters, lists all of them.
    #[proto(Tapepool.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the tape pool with this name
        name: str? => TapePool,
        /// Only the tape pools of this virtual organisation
        vo: str? => Vo,
        /// Only the encrypted (`true`) or unencrypted (`false`) tape pools
        encrypted: bool? => Encrypted,
        /// Only the tape pools using this encryption key
        encryption_key_name: str? => EncryptionKeyName,
    }
}
