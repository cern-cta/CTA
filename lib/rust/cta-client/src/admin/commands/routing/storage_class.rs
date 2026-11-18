// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Storage class commands

use crate::typed_admin_cmd;

/// Adds a storage class (`storageclass add`).
#[typed_admin_cmd(Storageclass::Add)]
pub struct Add {
    /// Name of the storage class
    #[proto(StorageClass)]
    pub name: String,
    /// Number of tape copies of the files of the storage class
    #[proto(CopyNumber)]
    pub number_of_copies: u64,
    /// Virtual organisation of the storage class
    pub vo: String,
    /// Comment
    pub comment: String,
}

/// Changes a storage class (`storageclass ch`).
#[typed_admin_cmd(Storageclass::Ch)]
pub struct Ch {
    /// Name of the storage class
    #[proto(StorageClass)]
    pub name: String,
    /// New number of tape copies
    #[proto(CopyNumber)]
    pub number_of_copies: Option<u64>,
    /// New virtual organisation
    pub vo: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a storage class (`storageclass rm`).
#[typed_admin_cmd(Storageclass::Rm)]
pub struct Rm {
    /// Name of the storage class
    #[proto(StorageClass)]
    pub name: String,
}

/// Lists storage classes (`storageclass ls`).
#[typed_admin_cmd(Storageclass::Ls, response = StorageClassLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the storage class with this name
    #[proto(StorageClass)]
    pub name: Option<String>,
    /// Only the storage classes of the files on this tape
    pub vid: Option<String>,
}
