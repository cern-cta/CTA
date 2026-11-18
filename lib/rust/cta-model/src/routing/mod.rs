// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! How files reach tape and when tapes are mounted: storage classes, archive routes, mount policies and the rules
//! assigning them to requesters

mod activity_mount_rule;
mod archive_route;
mod group_mount_rule;
mod mount_policy;
mod storage_class;
#[cfg(test)]
mod tests;
mod user_mount_rule;

pub use activity_mount_rule::ActivityMountRule;
pub use archive_route::{ArchiveRoute, ArchiveRouteType};
pub use group_mount_rule::GroupMountRule;
pub use mount_policy::MountPolicy;
pub use storage_class::StorageClass;
pub use user_mount_rule::UserMountRule;
