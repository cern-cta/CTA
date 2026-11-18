// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Virtual organisations

use cta_protobuf::cta::admin::VirtualOrganizationLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// A virtual organisation: a group of users with their own tape pools and drive quotas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct VirtualOrganisation {
    /// Name of the virtual organisation
    pub name: String,
    /// Disk instance the virtual organisation archives from and retrieves to
    pub disk_instance_name: String,
    /// Whether this is the virtual organisation of the repack workflow
    pub is_repack_vo: bool,
    /// Maximum number of drives used for retrieves
    pub max_read_drives: u64,
    /// Maximum number of drives used for archives
    pub max_write_drives: u64,
    /// Maximum size of an archived file, in bytes
    pub max_file_size: u64,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<VirtualOrganizationLsItem> for VirtualOrganisation {
    type Error = ConversionError;

    fn try_from(item: VirtualOrganizationLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            disk_instance_name: item.diskinstance,
            is_repack_vo: item.is_repack_vo,
            max_read_drives: item.read_max_drives,
            max_write_drives: item.write_max_drives,
            max_file_size: item.max_file_size,
            metadata: Metadata::convert(
                "VirtualOrganizationLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
