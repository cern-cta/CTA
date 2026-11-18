// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Mount policies

use std::time::Duration;

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::routing::MountPolicy;

use crate::admin::{
    AdminError, AdminStream,
    commands::mount_policy,
    dao::{find_one, secs},
};

/// Mount policies: see [`MountPolicy`].
#[admin_dao]
pub struct MountPolicies;

#[bon]
impl MountPolicies {
    /// The mount policy named `name`.
    pub async fn get(&self, name: &str) -> Result<MountPolicy, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().await?,
            "mount policy",
            name,
            move |policy: &MountPolicy| policy.name == wanted,
        )
        .await
    }

    /// All the mount policies.
    pub async fn list(&self) -> Result<AdminStream<MountPolicy>, AdminError> {
        self.client.stream_as(mount_policy::Ls::default()).await
    }

    /// Creates a mount policy.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Priority of the archive requests
        #[builder(default = 250)]
        archive_priority: u64,
        /// How long archive requests wait before a tape is mounted for them
        #[builder(default = Duration::from_secs(14_400))]
        min_archive_age: Duration,
        /// Priority of the retrieve requests
        #[builder(default = 150)]
        retrieve_priority: u64,
        /// How long retrieve requests wait before a tape is mounted for them
        #[builder(default = Duration::from_secs(14_400))]
        min_retrieve_age: Duration,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = mount_policy::Add::builder()
            .name(name)
            .archive_priority(archive_priority)
            .min_archive_request_age(secs(min_archive_age))
            .retrieve_priority(retrieve_priority)
            .min_retrieve_request_age(secs(min_retrieve_age))
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a mount policy; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New priority of the archive requests
        archive_priority: Option<u64>,
        /// New minimum age of the archive requests
        min_archive_age: Option<Duration>,
        /// New priority of the retrieve requests
        retrieve_priority: Option<u64>,
        /// New minimum age of the retrieve requests
        min_retrieve_age: Option<Duration>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = mount_policy::Ch::builder()
            .name(name)
            .maybe_archive_priority(archive_priority)
            .maybe_min_archive_request_age(min_archive_age.map(secs))
            .maybe_retrieve_priority(retrieve_priority)
            .maybe_min_retrieve_request_age(min_retrieve_age.map(secs))
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a mount policy.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(mount_policy::Rm::builder().name(name).build())
            .await
    }
}
