// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Files on tape and the recycle bin of deleted copies

use std::path::PathBuf;

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::file::{File, FileId};

use crate::admin::{
    AdminError, AdminStream,
    commands::tape_file,
    dao::{at_least_one, exactly_one, id_file::disk_file_ids_from},
};

mod recycle_bin;
#[cfg(test)]
mod tests;

pub use recycle_bin::RecycleBinTapeFiles;

/// Files on tape: see [`File`].
#[admin_dao]
pub struct TapeFiles;

#[bon]
impl TapeFiles {
    /// All the tape copies of a file.
    pub async fn get_all_copies(&self, id: impl Into<FileId>) -> Result<Vec<File>, AdminError> {
        let id = id.into();
        let cmd = tape_file::Ls::builder()
            .maybe_archive_file_id(id.archive_id())
            .maybe_disk_file_id(id.disk_file_id())
            .maybe_disk_instance_name(id.disk_instance_name())
            .build();
        at_least_one(self.client.stream_as(cmd).await?, "tape file", &id).await
    }

    /// The copy of a file on the tape `vid`.
    pub async fn get(&self, id: impl Into<FileId>, vid: &str) -> Result<File, AdminError> {
        let id = id.into();
        let cmd = tape_file::Ls::builder()
            .maybe_archive_file_id(id.archive_id())
            .maybe_disk_file_id(id.disk_file_id())
            .maybe_disk_instance_name(id.disk_instance_name())
            .vid(vid)
            .build();
        exactly_one(
            self.client.stream_as(cmd).await?,
            "tape file",
            format!("{id} on {vid}"),
        )
        .await
    }

    /// The files on a tape, or the files of a disk instance listed by their disk file IDs.
    #[builder]
    pub async fn list(
        &self,
        /// The files on this tape
        #[builder(into)]
        vid: Option<String>,
        /// The files with these disk file IDs (with `disk_instance_name`)
        disk_file_ids: Option<Vec<String>>,
        /// The files with the disk file IDs listed in this file (see [`crate::admin::dao::read_disk_file_ids`], with
        /// `disk_instance_name`)
        #[builder(into)]
        disk_file_ids_file: Option<PathBuf>,
        /// The disk instance of the disk file IDs
        #[builder(into)]
        disk_instance_name: Option<String>,
    ) -> Result<AdminStream<File>, AdminError> {
        let mut ids = disk_file_ids;
        if let Some(path) = disk_file_ids_file {
            ids.get_or_insert_default()
                .extend(disk_file_ids_from(&path)?);
        }
        let cmd = tape_file::Ls::builder()
            .maybe_vid(vid)
            .maybe_disk_file_ids(ids)
            .maybe_disk_instance_name(disk_instance_name)
            .build();
        self.client.stream_as(cmd).await
    }

    /// Deletes the copy of a file on the tape `vid`, giving a reason.
    pub async fn delete(
        &self,
        id: impl Into<FileId>,
        vid: &str,
        reason: &str,
    ) -> Result<(), AdminError> {
        let id = id.into();
        let cmd = tape_file::Rm::builder()
            .vid(vid)
            .maybe_archive_file_id(id.archive_id())
            .maybe_disk_file_id(id.disk_file_id())
            .maybe_disk_instance_name(id.disk_instance_name())
            .reason(reason)
            .build();
        self.client.run(cmd).await
    }
}
