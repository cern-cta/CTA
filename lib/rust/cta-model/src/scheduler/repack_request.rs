// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Repack requests

use std::time::Duration;

use chrono::{DateTime, Utc};
use cta_protobuf::cta::admin::{RepackDestinationInfos, RepackLsItem};
use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

use crate::{
    ConversionError, EntryLog, Keyed,
    convert::{optional_timestamp, required},
};

/// The state of a repack request.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "PascalCase")]
pub enum RepackState {
    /// Waiting to be picked up by the maintenance process
    Pending,
    /// Waiting for its files to be listed
    ToExpand,
    /// Picked up by a drive
    Starting,
    /// Files are being retrieved and archived
    Running,
    /// Every file was retrieved and archived
    Complete,
    /// Every file was processed, at least one failed
    Failed,
}

/// What a repack does with the files of the tape.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum RepackOperation {
    /// Only move the copies of the files to other tapes (`--justmove`)
    Move,
    /// Only create the copies missing on other tapes (`--justaddcopies`)
    AddCopies,
}

/// Counts of files and bytes of one phase of a repack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepackStatistics {
    /// Files to transfer
    pub total_file_count: u64,
    /// Bytes to transfer
    pub total_byte_count: u64,
    /// Files transferred
    pub transferred_file_count: u64,
    /// Bytes transferred
    pub transferred_byte_count: u64,
    /// Files whose transfer failed
    pub failed_file_count: u64,
    /// Bytes whose transfer failed
    pub failed_byte_count: u64,
}

impl RepackStatistics {
    /// Files left to transfer.
    #[must_use]
    pub fn remaining_file_count(&self) -> u64 {
        self.total_file_count
            .saturating_sub(self.transferred_file_count)
    }

    /// Bytes left to transfer.
    #[must_use]
    pub fn remaining_byte_count(&self) -> u64 {
        self.total_byte_count
            .saturating_sub(self.transferred_byte_count)
    }
}

/// What was on the tape when the repack started, and who started it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepackStartInfo {
    /// Files on the tape
    pub file_count: u64,
    /// Bytes on the tape
    pub byte_count: u64,
    /// Who submitted the repack, from where and when
    pub created: EntryLog,
    /// Whether every file of the tape was selected when the repack started
    pub is_all_files_selected_at_start: bool,
}

/// Files and bytes written to one destination tape by a repack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepackDestination {
    /// Volume identifier of the destination tape
    pub vid: String,
    /// Files written
    pub files: u64,
    /// Bytes written
    pub bytes: u64,
}

impl From<RepackDestinationInfos> for RepackDestination {
    fn from(item: RepackDestinationInfos) -> Self {
        Self {
            vid: item.vid,
            files: item.files,
            bytes: item.bytes,
        }
    }
}

/// A request to move the files of a tape to other tapes (`repack ls`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(vid)]
pub struct RepackRequest {
    /// Volume identifier of the tape being repacked
    pub vid: String,
    /// Tape pool of the tape
    pub tape_pool_name: String,
    /// State of the request
    pub state: RepackState,
    /// Files that failed to be repacked
    pub failed_file_count: u64,
    /// Disk buffer the files transit through
    pub disk_buffer_url: String,
    /// Time since the request was created
    pub duration: Duration,
    /// When the request reached its final state, if it did
    pub finished_time: Option<DateTime<Utc>>,
    /// Files and bytes written to each destination tape
    pub destination_infos: Vec<RepackDestination>,
    /// Sequence number of the last file listed for repacking
    pub last_expanded_fseq: u64,
    /// What was on the tape when the repack started
    pub start_info: RepackStartInfo,
    /// Counts of the retrieve phase
    pub retrieve_statistics: RepackStatistics,
    /// Counts of the archive phase
    pub archive_statistics: RepackStatistics,
}

impl RepackRequest {
    /// The progress of the repack (between 0 and 1): half for the retrieves, half for the archives.
    #[must_use]
    pub fn progress(&self) -> f64 {
        let retrieve = &self.retrieve_statistics;
        let archive = &self.archive_statistics;
        if retrieve.total_byte_count == 0 || archive.total_byte_count == 0 {
            return 0.0;
        }
        retrieve.transferred_byte_count as f64 / retrieve.total_byte_count as f64 / 2.0
            + archive.transferred_byte_count as f64 / archive.total_byte_count as f64 / 2.0
    }
}

impl TryFrom<RepackLsItem> for RepackRequest {
    type Error = ConversionError;

    fn try_from(item: RepackLsItem) -> Result<Self, Self::Error> {
        const RECORD: &str = "RepackLsItem";
        Ok(Self {
            vid: item.vid,
            tape_pool_name: item.tapepool,
            state: item
                .status
                .parse()
                .map_err(|_| ConversionError::invalid_value("status", &item.status))?,
            failed_file_count: item.total_failed_files,
            disk_buffer_url: item.repack_buffer_url,
            duration: Duration::from_secs(item.repack_time),
            finished_time: optional_timestamp("repack_finished_time", item.repack_finished_time)?,
            destination_infos: item.destination_infos.into_iter().map(Into::into).collect(),
            last_expanded_fseq: item.last_expanded_fseq,
            start_info: RepackStartInfo {
                file_count: item.total_files_on_tape_at_start,
                byte_count: item.total_bytes_on_tape_at_start,
                created: EntryLog::convert(
                    "creation_log",
                    required(RECORD, "creation_log", item.creation_log)?,
                )?,
                is_all_files_selected_at_start: item.all_files_selected_at_start,
            },
            retrieve_statistics: RepackStatistics {
                total_file_count: item.total_files_to_retrieve,
                total_byte_count: item.total_bytes_to_retrieve,
                transferred_file_count: item.retrieved_files,
                transferred_byte_count: item.retrieved_bytes,
                failed_file_count: item.failed_to_retrieve_files,
                failed_byte_count: item.failed_to_retrieve_bytes,
            },
            archive_statistics: RepackStatistics {
                total_file_count: item.total_files_to_archive,
                total_byte_count: item.total_bytes_to_archive,
                transferred_file_count: item.archived_files,
                transferred_byte_count: item.archived_bytes,
                failed_file_count: item.failed_to_archive_files,
                failed_byte_count: item.failed_to_archive_bytes,
            },
        })
    }
}
