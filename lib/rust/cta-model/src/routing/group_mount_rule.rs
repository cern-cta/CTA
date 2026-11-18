// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Group mount rules: the mount policy of the requests of a group

use cta_protobuf::cta::admin::GroupMountRuleLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// The mount policy of the requests of a group on a disk instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(group_name, disk_instance_name)]
pub struct GroupMountRule {
    /// Group name on the disk instance
    pub group_name: String,
    /// Disk instance
    pub disk_instance_name: String,
    /// Mount policy applied
    pub mount_policy_name: String,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<GroupMountRuleLsItem> for GroupMountRule {
    type Error = ConversionError;

    fn try_from(item: GroupMountRuleLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            group_name: item.group_mount_rule,
            disk_instance_name: item.disk_instance,
            mount_policy_name: item.mount_policy,
            metadata: Metadata::convert(
                "GroupMountRuleLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
