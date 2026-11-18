// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tapes (`tape`), tape pools and media types

use std::path::PathBuf;

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::tape::{Tape, TapeState, VerificationStatus};

use crate::admin::{
    AdminError, AdminStream,
    commands::tape,
    dao::{exactly_one, id_file::disk_file_ids_from},
};

mod media_type;
mod pool;
#[cfg(test)]
mod tests;

pub use media_type::MediaTypes;
pub use pool::TapePools;

/// Tapes (`tape`): see [`Tape`].
#[admin_dao]
pub struct Tapes;

#[bon]
impl Tapes {
    /// The tape with the volume identifier `vid`.
    pub async fn get(&self, vid: &str) -> Result<Tape, AdminError> {
        let cmd = tape::Ls::builder().vid(vid).build();
        exactly_one(self.client.stream_as(cmd).await?, "tape", vid).await
    }

    /// The tapes matching the filters (all of them without filters).
    #[builder]
    pub async fn list(
        &self,
        /// Only the tapes of this media type
        #[builder(into)]
        media_type_name: Option<String>,
        /// Only the tapes of this vendor
        #[builder(into)]
        vendor: Option<String>,
        /// Only the tapes of this logical library
        #[builder(into)]
        logical_library_name: Option<String>,
        /// Only the tapes of this tape pool
        #[builder(into)]
        tape_pool_name: Option<String>,
        /// Only the tapes of this virtual organisation
        #[builder(into)]
        virtual_organisation_name: Option<String>,
        /// Only the full (`true`) or not full (`false`) tapes
        full: Option<bool>,
        /// Only the tapes holding these disk files
        disk_file_ids: Option<Vec<String>>,
        /// Only the tapes holding the disk files listed in this file (see [`crate::admin::dao::read_disk_file_ids`])
        #[builder(into)]
        disk_file_ids_file: Option<PathBuf>,
        /// Only the tapes in this state
        state: Option<TapeState>,
        /// Only the tapes of this purchase order
        #[builder(into)]
        purchase_order: Option<String>,
        /// Only the tapes of this physical library
        #[builder(into)]
        physical_library_name: Option<String>,
        /// Only the tapes with files missing a copy
        #[builder(default)]
        missing_file_copies: bool,
    ) -> Result<AdminStream<Tape>, AdminError> {
        let mut ids = disk_file_ids;
        if let Some(path) = disk_file_ids_file {
            ids.get_or_insert_default()
                .extend(disk_file_ids_from(&path)?);
        }
        let cmd = tape::Ls::builder()
            .maybe_media_type(media_type_name)
            .maybe_vendor(vendor)
            .maybe_logical_library(logical_library_name)
            .maybe_tape_pool(tape_pool_name)
            .maybe_vo(virtual_organisation_name)
            .maybe_full(full)
            .maybe_disk_file_ids(ids)
            .maybe_state(state)
            .maybe_purchase_order(purchase_order)
            .maybe_physical_library(physical_library_name)
            .missing_file_copies(missing_file_copies)
            .build();
        // without a filter, `tape ls` wants an explicit `--all`
        let cmd = tape::Ls {
            all: cmd == tape::Ls::default(),
            ..cmd
        };
        self.client.stream_as(cmd).await
    }

    /// Creates a tape.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] vid: String,
        /// Media type
        #[builder(into)]
        media_type_name: String,
        /// Vendor
        #[builder(into)]
        vendor: String,
        /// Logical library
        #[builder(into)]
        logical_library_name: String,
        /// Tape pool
        #[builder(into)]
        tape_pool_name: String,
        /// Whether the tape is full
        #[builder(default)]
        full: bool,
        /// Purchase order
        #[builder(into)]
        purchase_order: Option<String>,
        /// Comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = tape::Add::builder()
            .vid(vid)
            .media_type(media_type_name)
            .vendor(vendor)
            .logical_library(logical_library_name)
            .tape_pool(tape_pool_name)
            .full(full)
            .maybe_purchase_order(purchase_order)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a tape; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] vid: String,
        /// New media type
        #[builder(into)]
        media_type_name: Option<String>,
        /// New vendor
        #[builder(into)]
        vendor: Option<String>,
        /// New logical library
        #[builder(into)]
        logical_library_name: Option<String>,
        /// New tape pool
        #[builder(into)]
        tape_pool_name: Option<String>,
        /// Result of the last verification
        verification_status: Option<VerificationStatus>,
        /// Whether the tape is full
        full: Option<bool>,
        /// New purchase order
        #[builder(into)]
        purchase_order: Option<String>,
        /// Whether the tape holds deleted files
        dirty: Option<bool>,
        /// New encryption key name
        #[builder(into)]
        encryption_key_name: Option<String>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = tape::Ch::builder()
            .vid(vid)
            .maybe_media_type(media_type_name)
            .maybe_vendor(vendor)
            .maybe_logical_library(logical_library_name)
            .maybe_tape_pool(tape_pool_name)
            .maybe_verification_status(verification_status.map(|status| status.to_json()))
            .maybe_full(full)
            .maybe_purchase_order(purchase_order)
            .maybe_dirty_bit(dirty)
            .maybe_encryption_key_name(encryption_key_name)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes the state of a tape, giving a reason.
    pub async fn change_state(
        &self,
        vid: &str,
        state: TapeState,
        reason: &str,
    ) -> Result<(), AdminError> {
        let cmd = tape::Ch::builder()
            .vid(vid)
            .state(state)
            .reason(reason)
            .build();
        self.client.run(cmd).await
    }

    /// Reclaims a tape: makes a full tape without files writable again.
    pub async fn reclaim(&self, vid: &str) -> Result<(), AdminError> {
        self.client
            .run(tape::Reclaim::builder().vid(vid).build())
            .await
    }

    /// Deletes a tape.
    pub async fn delete(&self, vid: &str) -> Result<(), AdminError> {
        self.client.run(tape::Rm::builder().vid(vid).build()).await
    }
}
