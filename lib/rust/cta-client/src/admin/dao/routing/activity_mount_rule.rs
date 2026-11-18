// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Mount rules of activities

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::routing::ActivityMountRule;

use crate::admin::{AdminError, AdminStream, commands::activity_mount_rule, dao::find_one};

/// Mount rules of activities: see [`ActivityMountRule`].
#[admin_dao]
pub struct ActivityMountRules;

#[bon]
impl ActivityMountRules {
    /// The mount rule of the activities of a user on a disk instance matching `regex`.
    pub async fn get(
        &self,
        user_name: &str,
        disk_instance_name: &str,
        regex: &str,
    ) -> Result<ActivityMountRule, AdminError> {
        let (user, instance, wanted_regex) = (
            user_name.to_owned(),
            disk_instance_name.to_owned(),
            regex.to_owned(),
        );
        find_one(
            self.list().await?,
            "activity mount rule",
            format!("{user_name}@{disk_instance_name} {regex}"),
            move |rule: &ActivityMountRule| {
                rule.user_name == user
                    && rule.disk_instance_name == instance
                    && rule.regex == wanted_regex
            },
        )
        .await
    }

    /// All the mount rules of activities.
    pub async fn list(&self) -> Result<AdminStream<ActivityMountRule>, AdminError> {
        self.client
            .stream_as(activity_mount_rule::Ls::default())
            .await
    }

    /// Creates the mount rule of the activities of a user on a disk instance matching `regex`.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] user_name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        #[builder(start_fn, into)] regex: String,
        /// Mount policy applied
        #[builder(into)]
        mount_policy_name: String,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = activity_mount_rule::Add::builder()
            .name(user_name)
            .instance(disk_instance_name)
            .activity_regex(regex)
            .mount_policy(mount_policy_name)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes the mount rule of an activity; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] user_name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        #[builder(start_fn, into)] regex: String,
        /// New mount policy
        #[builder(into)]
        mount_policy_name: Option<String>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = activity_mount_rule::Ch::builder()
            .name(user_name)
            .instance(disk_instance_name)
            .activity_regex(regex)
            .maybe_mount_policy(mount_policy_name)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes the mount rule of the activities of a user on a disk instance matching `regex`.
    pub async fn delete(
        &self,
        user_name: &str,
        disk_instance_name: &str,
        regex: &str,
    ) -> Result<(), AdminError> {
        let cmd = activity_mount_rule::Rm::builder()
            .name(user_name)
            .instance(disk_instance_name)
            .activity_regex(regex)
            .build();
        self.client.run(cmd).await
    }
}
