// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Virtual organisations

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::admin::VirtualOrganisation;

use crate::admin::{AdminError, AdminStream, commands::virtual_organization, dao::find_one};

/// Virtual organisations: see [`VirtualOrganisation`].
#[admin_dao]
pub struct VirtualOrganisations;

#[bon]
impl VirtualOrganisations {
    /// The virtual organisation named `name`.
    pub async fn get(&self, name: &str) -> Result<VirtualOrganisation, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().await?,
            "virtual organisation",
            name,
            move |vo: &VirtualOrganisation| vo.name == wanted,
        )
        .await
    }

    /// All the virtual organisations.
    pub async fn list(&self) -> Result<AdminStream<VirtualOrganisation>, AdminError> {
        self.client
            .stream_as(virtual_organization::Ls::default())
            .await
    }

    /// Creates a virtual organisation.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Disk instance the virtual organisation archives from and retrieves to
        #[builder(into)]
        disk_instance_name: String,
        /// Whether this is the virtual organisation of the repack workflow
        #[builder(default)]
        is_repack_vo: bool,
        /// Maximum number of drives used for retrieves
        #[builder(default = 1)]
        max_read_drives: u64,
        /// Maximum number of drives used for archives
        #[builder(default = 1)]
        max_write_drives: u64,
        /// Maximum size of an archived file, in bytes
        #[builder(default = 1_000_000_000_000)]
        max_file_size: u64,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = virtual_organization::Add::builder()
            .vo(name)
            .disk_instance(disk_instance_name)
            .is_repack_vo(is_repack_vo)
            .read_max_drives(max_read_drives)
            .write_max_drives(max_write_drives)
            .max_file_size(max_file_size)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a virtual organisation; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New disk instance
        #[builder(into)]
        disk_instance_name: Option<String>,
        /// Whether this is the virtual organisation of the repack workflow
        is_repack_vo: Option<bool>,
        /// New maximum number of drives used for retrieves
        max_read_drives: Option<u64>,
        /// New maximum number of drives used for archives
        max_write_drives: Option<u64>,
        /// New maximum size of an archived file, in bytes
        max_file_size: Option<u64>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = virtual_organization::Ch::builder()
            .vo(name)
            .maybe_disk_instance(disk_instance_name)
            .maybe_is_repack_vo(is_repack_vo)
            .maybe_read_max_drives(max_read_drives)
            .maybe_write_max_drives(max_write_drives)
            .maybe_max_file_size(max_file_size)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a virtual organisation.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(virtual_organization::Rm::builder().vo(name).build())
            .await
    }
}
