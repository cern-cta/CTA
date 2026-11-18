// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instances

use cta_protobuf::cta::admin::DiskInstanceLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// A disk storage instance files are archived from and retrieved to (`diskinstance ls`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct DiskInstance {
    /// Name of the disk instance
    pub name: String,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<DiskInstanceLsItem> for DiskInstance {
    type Error = ConversionError;

    fn try_from(item: DiskInstanceLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            metadata: Metadata::convert(
                "DiskInstanceLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
