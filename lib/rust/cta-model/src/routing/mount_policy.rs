// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Mount policies

use std::time::Duration;

use cta_protobuf::cta::admin::MountPolicyLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// How the scheduler prioritises the requests of a user, group or activity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct MountPolicy {
    /// Name of the mount policy
    pub name: String,
    /// Priority of the archive requests
    pub archive_priority: u64,
    /// How long archive requests wait before a tape is mounted for them
    pub min_archive_age: Duration,
    /// Priority of the retrieve requests
    pub retrieve_priority: u64,
    /// How long retrieve requests wait before a tape is mounted for them
    pub min_retrieve_age: Duration,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<MountPolicyLsItem> for MountPolicy {
    type Error = ConversionError;

    fn try_from(item: MountPolicyLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            archive_priority: item.archive_priority,
            min_archive_age: Duration::from_secs(item.archive_min_request_age),
            retrieve_priority: item.retrieve_priority,
            min_retrieve_age: Duration::from_secs(item.retrieve_min_request_age),
            metadata: Metadata::convert(
                "MountPolicyLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
