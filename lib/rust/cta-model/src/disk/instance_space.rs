// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instance spaces

use std::time::Duration;

use cta_protobuf::cta::admin::DiskInstanceSpaceLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// A space of a disk instance whose free space is monitored (`diskinstancespace ls`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name, disk_instance_name)]
pub struct DiskInstanceSpace {
    /// Name of the space
    pub name: String,
    /// Disk instance the space belongs to
    pub disk_instance_name: String,
    /// How the free space is queried (`<protocol>:<space>`)
    pub free_space_url: String,
    /// How often the free space is queried
    pub refresh_interval: Duration,
    /// Free space at the last query, in bytes
    pub free_space: u64,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<DiskInstanceSpaceLsItem> for DiskInstanceSpace {
    type Error = ConversionError;

    fn try_from(item: DiskInstanceSpaceLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            disk_instance_name: item.disk_instance,
            free_space_url: item.free_space_query_url,
            refresh_interval: Duration::from_secs(item.refresh_interval),
            free_space: item.free_space,
            metadata: Metadata::convert(
                "DiskInstanceSpaceLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
