// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instance spaces

use std::time::Duration;

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::disk::DiskInstanceSpace;

use crate::admin::{AdminError, AdminStream, commands::disk_instance_space, dao::find_one};

/// Disk instance spaces: see [`DiskInstanceSpace`].
#[admin_dao]
pub struct DiskInstanceSpaces;

#[bon]
impl DiskInstanceSpaces {
    /// The space `name` of a disk instance.
    pub async fn get(
        &self,
        name: &str,
        disk_instance_name: &str,
    ) -> Result<DiskInstanceSpace, AdminError> {
        let (wanted, instance) = (name.to_owned(), disk_instance_name.to_owned());
        find_one(
            self.list().await?,
            "disk instance space",
            format!("{name}@{disk_instance_name}"),
            move |space: &DiskInstanceSpace| {
                space.name == wanted && space.disk_instance_name == instance
            },
        )
        .await
    }

    /// All the disk instance spaces.
    pub async fn list(&self) -> Result<AdminStream<DiskInstanceSpace>, AdminError> {
        self.client
            .stream_as(disk_instance_space::Ls::default())
            .await
    }

    /// Creates the space `name` of a disk instance.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        /// How the free space is queried (`<protocol>:<space>`)
        #[builder(into)]
        free_space_url: String,
        /// How often the free space is queried
        #[builder(default = Duration::from_secs(10))]
        refresh_interval: Duration,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = disk_instance_space::Add::builder()
            .name(name)
            .disk_instance_name(disk_instance_name)
            .free_space_url(free_space_url)
            .refresh_interval(refresh_interval)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a disk instance space; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        #[builder(start_fn, into)] disk_instance_name: String,
        /// New free space query
        #[builder(into)]
        free_space_url: Option<String>,
        /// New refresh interval
        refresh_interval: Option<Duration>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = disk_instance_space::Ch::builder()
            .name(name)
            .disk_instance_name(disk_instance_name)
            .maybe_free_space_url(free_space_url)
            .maybe_refresh_interval(refresh_interval)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes the space `name` of a disk instance.
    pub async fn delete(&self, name: &str, disk_instance_name: &str) -> Result<(), AdminError> {
        let cmd = disk_instance_space::Rm::builder()
            .name(name)
            .disk_instance_name(disk_instance_name)
            .build();
        self.client.run(cmd).await
    }
}
