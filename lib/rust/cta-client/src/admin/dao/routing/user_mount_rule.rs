// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Mount rules of users

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::routing::UserMountRule;

use crate::admin::{AdminError, AdminStream, commands::requester_mount_rule, dao::find_one};

/// Mount rules of users: see [`UserMountRule`].
#[admin_dao]
pub struct UserMountRules;

#[bon]
impl UserMountRules {
    /// The mount rule of a user on a disk instance.
    pub async fn get(
        &self,
        user_name: &str,
        disk_instance_name: &str,
    ) -> Result<UserMountRule, AdminError> {
        let (user, instance) = (user_name.to_owned(), disk_instance_name.to_owned());
        find_one(
            self.list().await?,
            "user mount rule",
            format!("{user_name}@{disk_instance_name}"),
            move |rule: &UserMountRule| {
                rule.user_name == user && rule.disk_instance_name == instance
            },
        )
        .await
    }

    /// All the mount rules of users.
    pub async fn list(&self) -> Result<AdminStream<UserMountRule>, AdminError> {
        self.client
            .stream_as(requester_mount_rule::Ls::default())
            .await
    }

    /// Creates the mount rule of a user on a disk instance.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] user_name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        /// Mount policy applied
        #[builder(into)]
        mount_policy_name: String,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = requester_mount_rule::Add::builder()
            .name(user_name)
            .instance(disk_instance_name)
            .mount_policy(mount_policy_name)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes the mount rule of a user; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] user_name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        /// New mount policy
        #[builder(into)]
        mount_policy_name: Option<String>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = requester_mount_rule::Ch::builder()
            .name(user_name)
            .instance(disk_instance_name)
            .maybe_mount_policy(mount_policy_name)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes the mount rule of a user on a disk instance.
    pub async fn delete(
        &self,
        user_name: &str,
        disk_instance_name: &str,
    ) -> Result<(), AdminError> {
        let cmd = requester_mount_rule::Rm::builder()
            .name(user_name)
            .instance(disk_instance_name)
            .build();
        self.client.run(cmd).await
    }
}
