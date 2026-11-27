// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk systems

use std::time::Duration;

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::disk::DiskSystem;

use crate::admin::{AdminError, AdminStream, commands::disk_system, dao::find_one};

/// Disk systems: see [`DiskSystem`].
#[admin_dao]
pub struct DiskSystems;

#[bon]
impl DiskSystems {
    /// The disk system named `name`.
    pub async fn get(&self, name: &str) -> Result<DiskSystem, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().await?,
            "disk system",
            name,
            move |system: &DiskSystem| system.name == wanted,
        )
        .await
    }

    /// All the disk systems.
    pub async fn list(&self) -> Result<AdminStream<DiskSystem>, AdminError> {
        self.client.stream_as(disk_system::Ls::default()).await
    }

    /// Creates a disk system.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Disk instance
        #[builder(into)]
        disk_instance_name: String,
        /// Space of the disk instance
        #[builder(into)]
        disk_instance_space_name: String,
        /// Regular expression matching the files of the disk system
        #[builder(into)]
        file_regex: String,
        /// Free space to keep, in bytes
        #[builder(default = 1_000_000_000_000)]
        desired_min_free_space: u64,
        /// How long retrieves wait when the free space is below the target
        #[builder(default = Duration::from_secs(3600))]
        sleep_time: Duration,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = disk_system::Add::builder()
            .name(name)
            .disk_instance_name(disk_instance_name)
            .disk_instance_space_name(disk_instance_space_name)
            .file_regex(file_regex)
            .desired_min_free_space(desired_min_free_space)
            .sleep_time(sleep_time)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a disk system; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New regular expression
        #[builder(into)]
        file_regex: Option<String>,
        /// New free space to keep, in bytes
        desired_min_free_space: Option<u64>,
        /// New waiting time
        sleep_time: Option<Duration>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = disk_system::Ch::builder()
            .name(name)
            .maybe_file_regex(file_regex)
            .maybe_desired_min_free_space(desired_min_free_space)
            .maybe_sleep_time(sleep_time)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a disk system.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(disk_system::Rm::builder().name(name).build())
            .await
    }
}
