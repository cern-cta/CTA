// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Deleted files kept in the recycle bin

use std::path::PathBuf;

use bon::bon;
use chrono::{DateTime, Utc};
use cta_client_macros::admin_dao;
use cta_model::file::{File, FileId, RecycleBinTapeFile};

use crate::admin::{
    AdminError, AdminStream,
    commands::recycle_tape_file,
    dao::{at_least_one, exactly_one, id_file::disk_file_ids_from},
};

/// Deleted files kept in the recycle bin: see [`RecycleBinTapeFile`].
#[admin_dao]
pub struct RecycleBinTapeFiles;

/// The seconds since the Unix epoch of a time, as the commands take them.
fn unix_time(time: DateTime<Utc>) -> u64 {
    u64::try_from(time.timestamp()).unwrap_or_default()
}

#[bon]
impl RecycleBinTapeFiles {
    /// All the deleted tape copies of a file.
    pub async fn get_all_copies(
        &self,
        id: impl Into<FileId>,
    ) -> Result<Vec<RecycleBinTapeFile>, AdminError> {
        let id = id.into();
        let cmd = recycle_tape_file::Ls::builder()
            .maybe_archive_file_id(id.archive_file_id())
            .maybe_disk_file_id(id.disk_file_id())
            .maybe_instance(id.disk_instance())
            .build();
        at_least_one(
            self.client.stream_as(cmd).await?,
            "recycle bin tape file",
            &id,
        )
        .await
    }

    /// The deleted copy of a file on the tape `vid`.
    pub async fn get(
        &self,
        id: impl Into<FileId>,
        vid: &str,
    ) -> Result<RecycleBinTapeFile, AdminError> {
        let id = id.into();
        let cmd = recycle_tape_file::Ls::builder()
            .maybe_archive_file_id(id.archive_file_id())
            .maybe_disk_file_id(id.disk_file_id())
            .maybe_instance(id.disk_instance())
            .vid(vid)
            .build();
        exactly_one(
            self.client.stream_as(cmd).await?,
            "recycle bin tape file",
            format!("{id} on {vid}"),
        )
        .await
    }

    /// The deleted files matching the filters.
    #[builder]
    pub async fn list(
        &self,
        /// The files deleted from this tape
        #[builder(into)]
        vid: Option<String>,
        /// The file with this archive file ID
        archive_file_id: Option<u64>,
        /// The copies with this copy number
        copy_number: Option<u64>,
        /// The files with these disk file IDs (with `disk_instance_name`)
        disk_file_ids: Option<Vec<String>>,
        /// The files with the disk file IDs listed in this file (see [`crate::admin::dao::read_disk_file_ids`], with
        /// `disk_instance_name`)
        #[builder(into)]
        disk_file_ids_file: Option<PathBuf>,
        /// The disk instance of the disk file IDs
        #[builder(into)]
        disk_instance_name: Option<String>,
        /// The files of this virtual organisation
        #[builder(into)]
        virtual_organisation_name: Option<String>,
        /// The files deleted at or after this time
        min_deletion_time: Option<DateTime<Utc>>,
        /// The files deleted at or before this time
        max_deletion_time: Option<DateTime<Utc>>,
    ) -> Result<AdminStream<RecycleBinTapeFile>, AdminError> {
        let mut ids = disk_file_ids;
        if let Some(path) = disk_file_ids_file {
            ids.get_or_insert_default()
                .extend(disk_file_ids_from(&path)?);
        }
        let cmd = recycle_tape_file::Ls::builder()
            .maybe_vid(vid)
            .maybe_archive_file_id(archive_file_id)
            .maybe_copy_nb(copy_number)
            .maybe_disk_file_ids(ids)
            .maybe_instance(disk_instance_name)
            .maybe_vo(virtual_organisation_name)
            .maybe_log_unixtime_min(min_deletion_time.map(unix_time))
            .maybe_log_unixtime_max(max_deletion_time.map(unix_time))
            .build();
        self.client.stream_as(cmd).await
    }

    /// Restores a deleted tape copy in the catalogue, returning the message of the frontend.
    ///
    /// The copy is identified by the tape, disk instance, archive file ID and copy number of `file`; it is attached
    /// to `file.disk_file.id`, so callers that recreated the disk file must set it to the new ID first.
    pub async fn restore(&self, file: &File) -> Result<String, AdminError> {
        let cmd = recycle_tape_file::Restore::builder()
            .vid(&file.tape_file.vid)
            .instance(&file.disk_file.instance)
            .disk_file_id(&file.disk_file.id)
            .archive_file_id(file.archive_file.id)
            .copy_nb(u64::from(file.tape_file.copy_nb))
            .build();
        Ok(self.client.unary(cmd).await?.message_txt)
    }
}
