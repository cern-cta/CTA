// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Repack requests (`repack`)

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::scheduler::{RepackOperation, RepackRequest};

use crate::admin::{AdminError, AdminStream, commands::repack, dao::exactly_one};

/// Repack requests: see [`RepackRequest`].
#[admin_dao]
pub struct RepackRequests;

#[bon]
impl RepackRequests {
    /// The repack request of the tape `vid`.
    pub async fn get(&self, vid: &str) -> Result<RepackRequest, AdminError> {
        let cmd = repack::Ls::builder().vid(vid).build();
        exactly_one(self.client.stream_as(cmd).await?, "repack request", vid).await
    }

    /// All the repack requests.
    pub async fn list(&self) -> Result<AdminStream<RepackRequest>, AdminError> {
        self.client.stream_as(repack::Ls::default()).await
    }

    /// Submits the repack of the tape `vid`.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] vid: String,
        /// Mount policy of the retrieve and archive requests of the repack
        #[builder(into)]
        mount_policy_name: String,
        /// Do not retrieve the files already in the disk buffer
        #[builder(default)]
        no_recall: bool,
        /// Disk buffer the files transit through
        #[builder(into)]
        buffer_url: Option<String>,
        /// Maximum number of files to repack
        max_files_to_select: Option<u64>,
        /// Only move the files, or only add their missing copies (both by default)
        operation: Option<RepackOperation>,
    ) -> Result<(), AdminError> {
        let cmd = repack::Add::builder()
            .vid(vid)
            .mount_policy_name(mount_policy_name)
            .no_recall(no_recall)
            .maybe_disk_buffer_url(buffer_url)
            .maybe_max_files_to_select(max_files_to_select)
            .just_move(operation == Some(RepackOperation::Move))
            .just_add_copies(operation == Some(RepackOperation::AddCopies))
            .build();
        self.client.run(cmd).await
    }

    /// Deletes the repack request of the tape `vid`.
    pub async fn delete(&self, vid: &str) -> Result<(), AdminError> {
        self.client
            .run(repack::Rm::builder().vid(vid).build())
            .await
    }
}
