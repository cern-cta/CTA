// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Storage classes

use cta_protobuf::cta::admin::StorageClassLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// A storage class: how many tape copies the files of a virtual organisation get.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct StorageClass {
    /// Name of the storage class
    pub name: String,
    /// Virtual organisation the storage class belongs to
    pub virtual_organisation_name: String,
    /// Number of copies on distinct tapes
    pub replica_count: u64,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<StorageClassLsItem> for StorageClass {
    type Error = ConversionError;

    fn try_from(item: StorageClassLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            virtual_organisation_name: item.vo,
            replica_count: item.nb_copies,
            metadata: Metadata::convert(
                "StorageClassLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
