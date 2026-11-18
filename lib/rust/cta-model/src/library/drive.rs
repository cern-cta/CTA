// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape drives

use std::{collections::BTreeMap, time::Duration};

use cta_protobuf::cta::admin::{DriveLsItem, drive_ls_item::DriveStatus};
use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

use crate::{ConversionError, Keyed, scheduler::MountType};

/// Whether a drive is available to CTA.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DriveOperatingState {
    /// The drive mounts tapes
    Up,
    /// The drive does not mount tapes
    Down,
}

impl DriveOperatingState {
    /// The state of a `DriveStatus` field.
    fn convert(field: &'static str, status: DriveStatus) -> Result<Self, ConversionError> {
        match status {
            DriveStatus::Up => Ok(Self::Up),
            DriveStatus::Down => Ok(Self::Down),
            other => Err(ConversionError::invalid_value(field, other.as_str_name())),
        }
    }
}

/// What a drive is doing with the tape it has mounted.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum TapeSessionState {
    /// The session is starting
    Starting,
    /// The tape is being mounted
    Mounting,
    /// Data is being transferred
    Transferring,
    /// The session is cleaning up
    CleaningUp,
    /// The tape is being ejected from the drive
    Unloading,
    /// The tape is being put back to its slot
    Unmounting,
    /// Retrieved data is being written to disk after the tape was read
    DrainingToDisk,
}

impl TapeSessionState {
    /// The session state of a `DriveStatus`, `None` for the statuses of a drive without a session.
    fn convert(field: &'static str, status: DriveStatus) -> Result<Option<Self>, ConversionError> {
        Ok(Some(match status {
            DriveStatus::Starting => Self::Starting,
            DriveStatus::Mounting => Self::Mounting,
            DriveStatus::Transferring => Self::Transferring,
            DriveStatus::CleaningUp => Self::CleaningUp,
            DriveStatus::Unloading => Self::Unloading,
            DriveStatus::Unmounting => Self::Unmounting,
            DriveStatus::DrainingToDisk => Self::DrainingToDisk,
            DriveStatus::Up | DriveStatus::Down | DriveStatus::Probing | DriveStatus::Shutdown => {
                return Ok(None);
            }
            DriveStatus::UnknownDriveStatus => {
                return Err(ConversionError::invalid_value(field, status.as_str_name()));
            }
        }))
    }
}

/// A mount in progress on a drive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(id)]
pub struct TapeSession {
    /// Session identifier
    pub id: u64,
    /// Volume identifier of the mounted tape
    pub vid: String,
    /// Tape pool of the mounted tape
    pub tape_pool_name: String,
    /// Direction of the transfer
    pub mount_type: MountType,
    /// Number of files transferred so far
    pub file_transferred_count: u64,
    /// Number of bytes transferred so far
    pub byte_transferred_count: u64,
    /// Virtual organisation served
    pub virtual_organisation_name: String,
    /// What the drive is doing
    pub state: TapeSessionState,
    /// How long the drive has been in this state
    pub duration: Duration,
}

impl TapeSession {
    /// The transfer speed in bytes per second (over at least one second).
    #[must_use]
    pub fn speed(&self) -> f64 {
        self.byte_transferred_count as f64 / self.duration.as_secs_f64().max(1.0)
    }
}

/// Information about the `cta-taped` process serving a drive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DriveMetadata {
    /// Time since the process last reported its state
    pub last_refresh_age: Duration,
    /// Version of `cta-taped`
    pub cta_version: String,
    /// Name of the scheduler backend the process works on
    pub scheduler_namespace: String,
    /// Free-text comment of the operators
    pub comment: String,
}

/// A tape drive and what it is doing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct Drive {
    /// Drive name
    pub name: String,
    /// Logical library the drive belongs to
    pub logical_library_name: String,
    /// Tape server running the drive
    pub host_name: String,
    /// State the operators asked for
    pub desired_state: DriveOperatingState,
    /// Current state
    pub state: DriveOperatingState,
    /// Reason given for putting the drive down, if any
    pub state_reason: String,
    /// Device path on the tape server
    pub device_path: String,
    /// Physical library the drive is in
    pub physical_library_name: String,
    /// Slot of the drive in its library
    pub physical_library_slot: String,
    /// The mount in progress, if any
    pub tape_session: Option<TapeSession>,
    /// How long the drive has been in its current status
    pub state_change_age: Duration,
    /// Configuration of the `cta-taped` process, as `category.key` entries
    pub config: BTreeMap<String, String>,
    /// Information about the `cta-taped` process
    pub metadata: DriveMetadata,
}

impl TryFrom<DriveLsItem> for Drive {
    type Error = ConversionError;

    fn try_from(item: DriveLsItem) -> Result<Self, Self::Error> {
        let status = item.drive_status();
        let desired_state =
            DriveOperatingState::convert("desired_drive_state", item.desired_drive_state())?;
        let state_change_age = Duration::from_secs(item.drive_status_since);
        let tape_session = match (
            MountType::convert("mount_type", item.mount_type())?,
            TapeSessionState::convert("drive_status", status)?,
        ) {
            (Some(mount_type), Some(state)) => Some(TapeSession {
                id: item.session_id,
                vid: item.vid,
                tape_pool_name: item.tapepool,
                mount_type,
                file_transferred_count: item.files_transferred_in_session,
                byte_transferred_count: item.bytes_transferred_in_session,
                virtual_organisation_name: item.vo,
                state,
                duration: state_change_age,
            }),
            _ => None,
        };
        Ok(Self {
            name: item.drive_name,
            logical_library_name: item.logical_library,
            host_name: item.host,
            desired_state,
            state: if status == DriveStatus::Down {
                DriveOperatingState::Down
            } else {
                DriveOperatingState::Up
            },
            state_reason: item.reason,
            device_path: item.dev_file_name,
            physical_library_name: item.physical_library,
            physical_library_slot: item.raw_library_slot,
            tape_session,
            state_change_age,
            config: item
                .drive_config
                .into_iter()
                .map(|entry| (format!("{}.{}", entry.category, entry.key), entry.value))
                .collect(),
            metadata: DriveMetadata {
                last_refresh_age: Duration::from_secs(item.time_since_last_update),
                cta_version: item.cta_version,
                scheduler_namespace: item.scheduler_backend_name,
                comment: item.comment,
            },
        })
    }
}
