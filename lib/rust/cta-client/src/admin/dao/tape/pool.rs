// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape pools (`tapepool`)

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::tape::TapePool;

use crate::admin::{AdminError, AdminStream, commands::tape_pool, dao::exactly_one};

/// Tape pools: see [`TapePool`].
#[admin_dao]
pub struct TapePools;

#[bon]
impl TapePools {
    /// The tape pool named `name`.
    pub async fn get(&self, name: &str) -> Result<TapePool, AdminError> {
        let cmd = tape_pool::Ls::builder().name(name).build();
        exactly_one(self.client.stream_as(cmd).await?, "tape pool", name).await
    }

    /// The tape pools matching the filters (all of them without filters).
    #[builder]
    pub async fn list(
        &self,
        /// Only the pools of this virtual organisation
        #[builder(into)]
        virtual_organisation_name: Option<String>,
        /// Only the encrypted (`true`) or unencrypted (`false`) pools
        encrypted: Option<bool>,
    ) -> Result<AdminStream<TapePool>, AdminError> {
        let cmd = tape_pool::Ls::builder()
            .maybe_vo(virtual_organisation_name)
            .maybe_encrypted(encrypted)
            .build();
        self.client.stream_as(cmd).await
    }

    /// Creates a tape pool.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Virtual organisation owning the pool
        #[builder(into)]
        virtual_organisation_name: String,
        /// Number of partially filled tapes the pool should keep for writing
        #[builder(default = 1)]
        min_writable_tape_count: u64,
        /// Name of the key the tapes of the pool are encrypted with
        #[builder(into)]
        encryption_key_name: Option<String>,
        /// Pools supplying tapes to this one
        supply_tape_pool_names: Option<Vec<String>>,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = tape_pool::Add::builder()
            .name(name)
            .vo(virtual_organisation_name)
            .partial_tapes(min_writable_tape_count)
            .maybe_encryption_key_name(encryption_key_name)
            .maybe_supply(supply_tape_pool_names.map(|names| names.join(",")))
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a tape pool; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New virtual organisation
        #[builder(into)]
        virtual_organisation_name: Option<String>,
        /// New number of partially filled tapes to keep for writing
        min_writable_tape_count: Option<u64>,
        /// New list of supplying pools
        supply_tape_pool_names: Option<Vec<String>>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = tape_pool::Ch::builder()
            .name(name)
            .maybe_vo(virtual_organisation_name)
            .maybe_partial_tapes(min_writable_tape_count)
            .maybe_supply(supply_tape_pool_names.map(|names| names.join(",")))
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Stops encrypting the tapes of a pool.
    pub async fn remove_encryption(&self, name: &str) -> Result<(), AdminError> {
        self.set_encryption_key_name(name, "").await
    }

    /// Sets the key the tapes of a pool are encrypted with.
    pub async fn set_encryption_key_name(
        &self,
        name: &str,
        encryption_key_name: &str,
    ) -> Result<(), AdminError> {
        let cmd = tape_pool::Ch::builder()
            .name(name)
            .encryption_key_name(encryption_key_name)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a tape pool.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(tape_pool::Rm::builder().name(name).build())
            .await
    }
}
