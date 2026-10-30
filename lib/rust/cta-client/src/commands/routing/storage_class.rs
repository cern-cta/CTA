// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Storage class commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a storage class (`storageclass add`).
    #[proto(Storageclass.SubcmdAdd)]
    pub struct Add {
        /// Name of the storage class
        name: str => StorageClass,
        /// Number of tape copies of the files of the storage class
        number_of_copies: u64 => CopyNumber,
        /// Virtual organisation of the storage class
        vo: str => Vo,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a storage class (`storageclass ch`).
    #[proto(Storageclass.SubcmdCh)]
    pub struct Ch {
        /// Name of the storage class
        name: str => StorageClass,
        /// New number of tape copies
        number_of_copies: u64? => CopyNumber,
        /// New virtual organisation
        vo: str? => Vo,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a storage class (`storageclass rm`).
    #[proto(Storageclass.SubcmdRm)]
    pub struct Rm {
        /// Name of the storage class
        name: str => StorageClass,
    }
}

define_typed_admin_cmd! {
    /// Lists storage classes (`storageclass ls`).
    #[proto(Storageclass.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the storage class with this name
        name: str? => StorageClass,
        /// Only the storage classes of the files on this tape
        vid: str? => Vid,
    }
}
