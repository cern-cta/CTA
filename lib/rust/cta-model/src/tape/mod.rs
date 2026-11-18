// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape cartridges, the tape pools they belong to, and their media types

use chrono::{DateTime, Utc};
use cta_protobuf::cta::{
    admin::{TapeLsItem, tape_ls_item::LabelFormat as ProtoLabelFormat},
    common,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::{
    ConversionError, EntryLog, Keyed, Metadata,
    convert::{non_empty, required, timestamp},
};

mod media_type;
mod pool;
#[cfg(test)]
mod tests;

pub use media_type::MediaType;
pub use pool::{TapePool, TapePoolStatistics};

/// The state of a tape, deciding whether it can be mounted.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum TapeState {
    /// The tape can be read from and written to
    Active,
    /// The tape cannot be mounted, but retrieve requests can still be queued for it
    Disabled,
    /// The tape is damaged; most likely its final state
    Broken,
    /// The tape was removed from the library
    Exported,
    /// The tape is being, or about to be, repacked
    Repacking,
    /// Like [`TapeState::Disabled`], for a tape being repacked
    RepackingDisabled,
}

/// The format of the labels and headers written on a tape.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
pub enum LabelFormat {
    /// CTA's own format
    #[strum(serialize = "CTA")]
    Cta,
    /// Open Storage Manager
    #[strum(serialize = "OSM")]
    Osm,
    /// Enstore
    Enstore,
    /// Enstore with large files
    EnstoreLarge,
}

impl From<ProtoLabelFormat> for LabelFormat {
    fn from(value: ProtoLabelFormat) -> Self {
        match value {
            ProtoLabelFormat::Cta => Self::Cta,
            ProtoLabelFormat::Osm => Self::Osm,
            ProtoLabelFormat::Enstore => Self::Enstore,
            ProtoLabelFormat::EnstoreLarge => Self::EnstoreLarge,
        }
    }
}

/// Which drive did something to a tape, and when (the `TapeLog` of the catalogue).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TapeLog {
    /// Drive name
    pub drive: String,
    /// When
    pub time: DateTime<Utc>,
}

impl TapeLog {
    /// Converts the protobuf log, `field` naming it in the error.
    fn convert(
        field: &'static str,
        log: Option<common::TapeLog>,
    ) -> Result<Option<Self>, ConversionError> {
        log.map(|log| {
            Ok(Self {
                drive: log.drive,
                time: timestamp(field, log.time)?,
            })
        })
        .transpose()
    }
}

/// The result of the last verification of a tape, as set by the operators (`tape ch --verificationstatus`).
///
/// The catalogue stores it as JSON (`{"date": ..., "status": ..., "files_submitted": ..., ...}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationStatus {
    /// When the verification took place
    pub date: DateTime<Utc>,
    /// Outcome (e.g. `finished`)
    pub status: String,
    /// Number of files submitted for verification
    pub file_submitted_count: u64,
    /// Number of files verified
    pub file_verified_count: u64,
    /// Number of files that failed the verification
    pub file_failed_count: u64,
}

/// The JSON representation of a [`VerificationStatus`].
#[derive(Deserialize)]
struct VerificationStatusJson {
    date: u64,
    status: String,
    files_submitted: u64,
    files_verified: u64,
    files_failed: u64,
}

impl VerificationStatus {
    /// Parses the JSON of the `verification_status` field; an empty field is `None`.
    ///
    /// # Errors
    ///
    /// [`ConversionError::InvalidJson`] when the field is not the expected JSON object,
    /// [`ConversionError::InvalidTimestamp`] when its date is out of range.
    pub fn from_json(json: &str) -> Result<Option<Self>, ConversionError> {
        const FIELD: &str = "verification_status";
        if json.trim().is_empty() {
            return Ok(None);
        }
        let parsed: VerificationStatusJson =
            serde_json::from_str(json).map_err(|source| ConversionError::InvalidJson {
                field: FIELD,
                source,
            })?;
        Ok(Some(Self {
            date: timestamp(FIELD, parsed.date)?,
            status: parsed.status,
            file_submitted_count: parsed.files_submitted,
            file_verified_count: parsed.files_verified,
            file_failed_count: parsed.files_failed,
        }))
    }

    /// The JSON the catalogue stores, as expected by `tape ch --verificationstatus`.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::json!({
            "date": u64::try_from(self.date.timestamp()).unwrap_or_default(),
            "status": self.status,
            "files_submitted": self.file_submitted_count,
            "files_verified": self.file_verified_count,
            "files_failed": self.file_failed_count,
        })
        .to_string()
    }
}

/// Usage statistics of a tape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TapeStatistics {
    /// Bytes written on the tape, including the files since deleted
    pub physical_usage: u64,
    /// Bytes of the files currently on the tape
    pub logical_usage: u64,
    /// Whether the tape holds deleted files (and would gain space from a repack)
    pub is_dirty: bool,
    /// Sequence number of the last file written
    pub last_fseq: u64,
    /// Whether the tape has no room left for writing
    pub is_full: bool,
    /// Number of files currently on the tape
    pub file_count: u64,
}

/// The logs of a tape: who created and modified it, which drives labelled, wrote and read it last, who changed its
/// state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TapeMetadata {
    /// Who created the tape, from where and when
    pub created: EntryLog,
    /// Who last modified the tape, from where and when
    pub modified: EntryLog,
    /// Free-text comment of the operators
    pub comment: String,
    /// Which drive labelled the tape and when, if it was labelled
    pub label: Option<TapeLog>,
    /// Which drive wrote the tape last and when, if it was written
    pub last_written: Option<TapeLog>,
    /// Which drive read the tape last and when, if it was read
    pub last_read: Option<TapeLog>,
    /// When the state was last changed
    pub state_update_time: DateTime<Utc>,
    /// Who last changed the state (`user@host`)
    pub state_modified_by: String,
    /// Number of mounts for writing
    pub write_mount_count: u64,
    /// Number of mounts for reading
    pub read_mount_count: u64,
}

impl TapeMetadata {
    /// The creation and modification logs and the comment, as a [`Metadata`].
    #[must_use]
    pub fn metadata(&self) -> Metadata {
        Metadata {
            created: self.created.clone(),
            modified: self.modified.clone(),
            comment: self.comment.clone(),
        }
    }
}

/// A tape cartridge (`tape ls`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(vid)]
pub struct Tape {
    /// Volume identifier
    pub vid: String,
    /// Media type
    pub media_type_name: String,
    /// Vendor
    pub vendor: String,
    /// Purchase order
    pub purchase_order: String,
    /// Logical library the tape belongs to
    pub logical_library_name: String,
    /// Physical library the tape is in
    pub physical_library_name: String,
    /// Tape pool the tape belongs to
    pub tape_pool_name: String,
    /// Virtual organisation of the tape pool
    pub virtual_organisation_name: String,
    /// Name of the key the tape is encrypted with (`None`: not encrypted)
    pub encryption_key_name: Option<String>,
    /// Capacity, in bytes
    pub capacity: u64,
    /// Format of the labels and headers
    pub format: LabelFormat,
    /// State
    pub state: TapeState,
    /// Reason given for the state
    pub state_reason: String,
    /// Result of the last verification, if any
    pub verification_status: Option<VerificationStatus>,
    /// Usage statistics
    pub statistics: TapeStatistics,
    /// Logs
    pub metadata: TapeMetadata,
}

impl Tape {
    /// The used fraction of the tape (between 0 and 1): the logical usage over the physical usage of a full tape, or
    /// over the capacity (or the physical usage when it exceeds it) of a tape still being written.
    #[must_use]
    pub fn usage_ratio(&self) -> f64 {
        let stats = &self.statistics;
        let total = if stats.is_full {
            // tape full: the (confirmed) total capacity is the physical usage,
            // which may exceed the theoretical capacity of a full tape
            stats.physical_usage.max(1)
        } else {
            // tape not full: the (estimated) total capacity is the configured capacity of the tape,
            // or the physical usage if it exceeds it
            stats.physical_usage.max(self.capacity)
        };
        stats.logical_usage as f64 / total.max(1) as f64
    }

    /// Whether nothing was ever written on the tape.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let stats = &self.statistics;
        !stats.is_full && stats.physical_usage == 0 && stats.last_fseq == 0 && stats.file_count == 0
    }
}

impl TryFrom<TapeLsItem> for Tape {
    type Error = ConversionError;

    fn try_from(item: TapeLsItem) -> Result<Self, Self::Error> {
        const RECORD: &str = "TapeLsItem";
        let format = item.label_format().into();
        Ok(Self {
            vid: item.vid,
            media_type_name: item.media_type,
            vendor: item.vendor,
            purchase_order: item.purchase_order,
            logical_library_name: item.logical_library,
            physical_library_name: item.physical_library,
            tape_pool_name: item.tapepool,
            virtual_organisation_name: item.vo,
            encryption_key_name: non_empty(item.encryption_key_name),
            capacity: item.capacity,
            format,
            state: item
                .state
                .parse()
                .map_err(|_| ConversionError::invalid_value("state", &item.state))?,
            state_reason: item.state_reason,
            verification_status: VerificationStatus::from_json(&item.verification_status)?,
            statistics: TapeStatistics {
                physical_usage: item.occupancy,
                logical_usage: item.master_data_in_bytes,
                is_dirty: item.dirty,
                last_fseq: item.last_fseq,
                is_full: item.full,
                file_count: item.nb_master_files,
            },
            metadata: TapeMetadata {
                created: EntryLog::convert(
                    "creation_log",
                    required(RECORD, "creation_log", item.creation_log)?,
                )?,
                modified: EntryLog::convert(
                    "last_modification_log",
                    required(RECORD, "last_modification_log", item.last_modification_log)?,
                )?,
                comment: item.comment,
                label: TapeLog::convert("label_log", item.label_log)?,
                last_written: TapeLog::convert("last_written_log", item.last_written_log)?,
                last_read: TapeLog::convert("last_read_log", item.last_read_log)?,
                state_update_time: timestamp("state_update_time", item.state_update_time)?,
                state_modified_by: item.state_modified_by,
                write_mount_count: item.write_mount_count,
                read_mount_count: item.read_mount_count,
            },
        })
    }
}
