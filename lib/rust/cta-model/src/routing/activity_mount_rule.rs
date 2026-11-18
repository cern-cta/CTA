// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Activity mount rules: the mount policy of the requests of an activity

use cta_protobuf::cta::admin::ActivityMountRuleLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// The mount policy of the requests of a user whose activity matches a regular expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(user_name, disk_instance_name, regex)]
pub struct ActivityMountRule {
    /// User name on the disk instance
    pub user_name: String,
    /// Disk instance
    pub disk_instance_name: String,
    /// Regular expression the activity of the request must match
    pub regex: String,
    /// Mount policy applied
    pub mount_policy_name: String,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<ActivityMountRuleLsItem> for ActivityMountRule {
    type Error = ConversionError;

    fn try_from(item: ActivityMountRuleLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            user_name: item.activity_mount_rule,
            disk_instance_name: item.disk_instance,
            regex: item.activity_regex,
            mount_policy_name: item.mount_policy,
            metadata: Metadata::convert(
                "ActivityMountRuleLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
