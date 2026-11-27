// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Storage classes

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::routing::StorageClass;

use crate::admin::{AdminError, AdminStream, commands::storage_class, dao::exactly_one};

/// Storage classes: see [`StorageClass`].
#[admin_dao]
pub struct StorageClasses;

#[bon]
impl StorageClasses {
    /// The storage class named `name`.
    pub async fn get(&self, name: &str) -> Result<StorageClass, AdminError> {
        let cmd = storage_class::Ls::builder().name(name).build();
        exactly_one(self.client.stream_as(cmd).await?, "storage class", name).await
    }

    /// The storage classes (all of them, or those with files on a tape).
    #[builder]
    pub async fn list(
        &self,
        /// Only the storage classes with files on this tape
        #[builder(into)]
        vid: Option<String>,
    ) -> Result<AdminStream<StorageClass>, AdminError> {
        let cmd = storage_class::Ls::builder().maybe_vid(vid).build();
        self.client.stream_as(cmd).await
    }

    /// Creates a storage class.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Virtual organisation the storage class belongs to
        #[builder(into)]
        virtual_organisation_name: String,
        /// Number of copies on distinct tapes
        #[builder(default = 1)]
        replica_count: u64,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = storage_class::Add::builder()
            .name(name)
            .virtual_organisation_name(virtual_organisation_name)
            .replica_count(replica_count)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a storage class; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New virtual organisation
        #[builder(into)]
        virtual_organisation_name: Option<String>,
        /// New number of copies
        replica_count: Option<u64>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = storage_class::Ch::builder()
            .name(name)
            .maybe_virtual_organisation_name(virtual_organisation_name)
            .maybe_replica_count(replica_count)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a storage class.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(storage_class::Rm::builder().name(name).build())
            .await
    }
}
