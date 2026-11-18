// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! User mount rules: the mount policy of the requests of a user

use cta_protobuf::cta::admin::RequesterMountRuleLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// The mount policy of the requests of a user on a disk instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(user_name, disk_instance_name)]
pub struct UserMountRule {
    /// User name on the disk instance
    pub user_name: String,
    /// Disk instance
    pub disk_instance_name: String,
    /// Mount policy applied
    pub mount_policy_name: String,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<RequesterMountRuleLsItem> for UserMountRule {
    type Error = ConversionError;

    fn try_from(item: RequesterMountRuleLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            user_name: item.requester_mount_rule,
            disk_instance_name: item.disk_instance,
            mount_policy_name: item.mount_policy,
            metadata: Metadata::convert(
                "RequesterMountRuleLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
