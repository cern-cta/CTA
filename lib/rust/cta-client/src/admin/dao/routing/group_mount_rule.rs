// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Mount rules of groups

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::routing::GroupMountRule;

use crate::admin::{AdminError, AdminStream, commands::group_mount_rule, dao::find_one};

/// Mount rules of groups: see [`GroupMountRule`].
#[admin_dao]
pub struct GroupMountRules;

#[bon]
impl GroupMountRules {
    /// The mount rule of a group on a disk instance.
    pub async fn get(
        &self,
        group_name: &str,
        disk_instance_name: &str,
    ) -> Result<GroupMountRule, AdminError> {
        let (group, instance) = (group_name.to_owned(), disk_instance_name.to_owned());
        find_one(
            self.list().await?,
            "group mount rule",
            format!("{group_name}@{disk_instance_name}"),
            move |rule: &GroupMountRule| {
                rule.group_name == group && rule.disk_instance_name == instance
            },
        )
        .await
    }

    /// All the mount rules of groups.
    pub async fn list(&self) -> Result<AdminStream<GroupMountRule>, AdminError> {
        self.client.stream_as(group_mount_rule::Ls::default()).await
    }

    /// Creates the mount rule of a group on a disk instance.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] group_name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        /// Mount policy applied
        #[builder(into)]
        mount_policy_name: String,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = group_mount_rule::Add::builder()
            .name(group_name)
            .instance(disk_instance_name)
            .mount_policy(mount_policy_name)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes the mount rule of a group; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] group_name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        /// New mount policy
        #[builder(into)]
        mount_policy_name: Option<String>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = group_mount_rule::Ch::builder()
            .name(group_name)
            .instance(disk_instance_name)
            .maybe_mount_policy(mount_policy_name)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes the mount rule of a group on a disk instance.
    pub async fn delete(
        &self,
        group_name: &str,
        disk_instance_name: &str,
    ) -> Result<(), AdminError> {
        let cmd = group_mount_rule::Rm::builder()
            .name(group_name)
            .instance(disk_instance_name)
            .build();
        self.client.run(cmd).await
    }
}
