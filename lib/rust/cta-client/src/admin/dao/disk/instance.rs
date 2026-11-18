// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instances

use cta_client_macros::admin_dao;
use cta_model::disk::DiskInstance;

use crate::admin::{AdminError, AdminStream, commands::disk_instance, dao::find_one};

/// Disk instances: see [`DiskInstance`].
#[admin_dao]
pub struct DiskInstances;

impl DiskInstances {
    /// The disk instance named `name`.
    pub async fn get(&self, name: &str) -> Result<DiskInstance, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().await?,
            "disk instance",
            name,
            move |instance: &DiskInstance| instance.name == wanted,
        )
        .await
    }

    /// All the disk instances.
    pub async fn list(&self) -> Result<AdminStream<DiskInstance>, AdminError> {
        self.client.stream_as(disk_instance::Ls::default()).await
    }

    /// Creates a disk instance.
    pub async fn create(&self, name: &str, comment: &str) -> Result<(), AdminError> {
        let cmd = disk_instance::Add::builder()
            .name(name)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes the comment of a disk instance.
    pub async fn update(&self, name: &str, comment: &str) -> Result<(), AdminError> {
        let cmd = disk_instance::Ch::builder()
            .name(name)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a disk instance.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(disk_instance::Rm::builder().name(name).build())
            .await
    }
}
