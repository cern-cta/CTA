// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape drives (`drive`)

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::library::Drive;

use crate::admin::{AdminError, AdminStream, commands::drive, dao::exactly_one};

/// Tape drives (`drive`): see [`Drive`].
#[admin_dao]
pub struct Drives;

#[bon]
impl Drives {
    /// The drive named `name`.
    pub async fn get(&self, name: &str) -> Result<Drive, AdminError> {
        let cmd = drive::Ls::builder().name(name).build();
        exactly_one(self.client.stream_as(cmd).await?, "drive", name).await
    }

    /// The drives of the enabled logical libraries, or all of them.
    #[builder]
    pub async fn list(
        &self,
        /// Include the drives of the disabled logical libraries
        #[builder(default)]
        all: bool,
    ) -> Result<AdminStream<Drive>, AdminError> {
        let cmd = drive::Ls::builder().all(all).build();
        self.client.stream_as(cmd).await
    }

    /// Changes the comment of a drive.
    pub async fn update(&self, name: &str, comment: &str) -> Result<(), AdminError> {
        let cmd = drive::Ch::builder().name(name).comment(comment).build();
        self.client.run(cmd).await
    }

    /// Puts a drive down, giving a reason.
    #[builder]
    pub async fn down(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Why the drive is put down
        #[builder(into)]
        reason: String,
        /// Put the drive down even if it is in a session
        #[builder(default)]
        force: bool,
    ) -> Result<(), AdminError> {
        let cmd = drive::Down::builder()
            .name(name)
            .reason(reason)
            .force(force)
            .build();
        self.client.run(cmd).await
    }

    /// Puts a drive up.
    #[builder]
    pub async fn up(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Why the drive is put up
        #[builder(into)]
        reason: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = drive::Up::builder().name(name).maybe_reason(reason).build();
        self.client.run(cmd).await
    }

    /// Deletes a drive.
    #[builder]
    pub async fn delete(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Delete the drive even if it is not down
        #[builder(default)]
        force: bool,
    ) -> Result<(), AdminError> {
        let cmd = drive::Rm::builder().name(name).force(force).build();
        self.client.run(cmd).await
    }
}
