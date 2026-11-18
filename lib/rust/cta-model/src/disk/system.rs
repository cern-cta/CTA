// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk systems

use std::time::Duration;

use cta_protobuf::cta::admin::DiskSystemLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// A directory tree of a disk instance space, whose retrieves wait when the space runs low (`disksystem ls`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct DiskSystem {
    /// Name of the disk system
    pub name: String,
    /// Disk instance
    pub disk_instance_name: String,
    /// Space of the disk instance
    pub disk_instance_space_name: String,
    /// Regular expression matching the files of the disk system
    pub file_regex: String,
    /// Free space to keep, in bytes
    pub desired_min_free_space: u64,
    /// How long retrieves wait when the free space is below the target
    pub sleep_time: Duration,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<DiskSystemLsItem> for DiskSystem {
    type Error = ConversionError;

    fn try_from(item: DiskSystemLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            disk_instance_name: item.disk_instance,
            disk_instance_space_name: item.disk_instance_space,
            file_regex: item.file_regexp,
            desired_min_free_space: item.targeted_free_space,
            sleep_time: Duration::from_secs(item.sleep_time),
            metadata: Metadata::convert(
                "DiskSystemLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
