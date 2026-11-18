// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The queues of the scheduler

use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use chrono::{DateTime, Utc};
use cta_protobuf::cta::admin::ShowQueuesItem;
use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

use crate::{
    ConversionError, Keyed,
    convert::{non_empty, optional_timestamp, required},
    scheduler::MountType,
};

/// A queue of the scheduler: the pending requests for one tape pool (archives) or one tape (retrieves).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(mount_type, tape_pool_name, vid)]
pub struct UserRequest {
    /// Direction of the transfers
    pub mount_type: MountType,
    /// Tape pool the requests are for
    pub tape_pool_name: String,
    /// Tape the requests are for (retrieves only)
    pub vid: Option<String>,
    /// Virtual organisation of the requests
    pub virtual_organisation_name: String,
    /// Logical library of the tape (retrieves only)
    pub logical_library_name: String,
    /// Files queued
    pub queued_file_count: u64,
    /// Bytes queued
    pub queued_byte_count: u64,
    /// Age of the oldest request
    pub oldest_age: Duration,
    /// Highest priority among the mount policies of the requests
    pub priority: u64,
    /// Lowest minimum request age among the mount policies of the requests
    pub min_age: Duration,
    /// Drives currently mounted for this queue
    pub mount_count: u64,
    /// Files transferred by the current mounts
    pub transferred_file_count: u64,
    /// Bytes transferred by the current mounts
    pub transferred_byte_count: u64,
    /// Whether the queue waits for free space on disk
    pub is_sleeping_for_space: bool,
    /// When the queue started waiting for free space
    pub sleep_start_time: Option<DateTime<Utc>>,
    /// Scheduler backend holding the queue
    pub scheduler_backend_name: String,
    /// Files on the tapes of the pool (archives only)
    pub tapes_file_count: u64,
    /// Bytes on the tapes of the pool (archives only)
    pub tapes_byte_count: u64,
    /// Capacity of the tapes of the pool (archives only)
    pub tapes_capacity: u64,
}

impl TryFrom<ShowQueuesItem> for UserRequest {
    type Error = ConversionError;

    fn try_from(item: ShowQueuesItem) -> Result<Self, Self::Error> {
        Ok(Self {
            mount_type: required(
                "ShowQueuesItem",
                "mount_type",
                MountType::convert("mount_type", item.mount_type())?,
            )?,
            tape_pool_name: item.tapepool,
            vid: non_empty(item.vid),
            virtual_organisation_name: item.vo,
            logical_library_name: item.logical_library,
            queued_file_count: item.queued_files,
            queued_byte_count: item.queued_bytes,
            oldest_age: Duration::from_secs(item.oldest_age),
            priority: item.priority,
            min_age: Duration::from_secs(item.min_age),
            mount_count: item.cur_mounts,
            transferred_file_count: item.cur_files,
            transferred_byte_count: item.cur_bytes,
            is_sleeping_for_space: item.sleeping_for_space,
            sleep_start_time: optional_timestamp("sleep_start_time", item.sleep_start_time)?,
            scheduler_backend_name: item.scheduler_backend_name,
            tapes_file_count: item.tapes_files,
            tapes_byte_count: item.tapes_bytes,
            tapes_capacity: item.tapes_capacity,
        })
    }
}

/// The field the queues are grouped by in a [`summarize`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum GroupBy {
    /// [`UserRequest::tape_pool_name`]
    TapePoolName,
    /// [`UserRequest::logical_library_name`]
    LogicalLibraryName,
    /// [`UserRequest::virtual_organisation_name`]
    VirtualOrganisationName,
}

impl GroupBy {
    /// The value of the grouping field of `request`.
    fn value(self, request: &UserRequest) -> &str {
        match self {
            Self::TapePoolName => &request.tape_pool_name,
            Self::LogicalLibraryName => &request.logical_library_name,
            Self::VirtualOrganisationName => &request.virtual_organisation_name,
        }
    }
}

/// The field the summaries are sorted by in a [`summarize`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum OrderBy {
    /// [`UserRequestSummary::min_age`]
    MinAge,
    /// [`UserRequestSummary::oldest_age`]
    OldestAge,
    /// [`UserRequestSummary::queued_file_count`]
    QueuedFileCount,
    /// [`UserRequestSummary::queued_byte_count`]
    QueuedByteCount,
    /// [`UserRequestSummary::mount_count`]
    MountCount,
    /// [`UserRequestSummary::transferred_file_count`]
    TransferredFileCount,
    /// [`UserRequestSummary::transferred_byte_count`]
    TransferredByteCount,
    /// [`UserRequestSummary::tapes_count`]
    TapesCount,
    /// [`UserRequestSummary::tapes_file_count`]
    TapesFileCount,
    /// [`UserRequestSummary::tapes_byte_count`]
    TapesByteCount,
    /// [`UserRequestSummary::tapes_capacity`]
    TapesCapacity,
    /// [`UserRequestSummary::highest_priority`]
    HighestPriority,
}

impl OrderBy {
    /// The sort key of `summary`.
    fn value(self, summary: &UserRequestSummary) -> u64 {
        match self {
            Self::MinAge => summary.min_age.as_secs(),
            Self::OldestAge => summary.oldest_age.as_secs(),
            Self::QueuedFileCount => summary.queued_file_count,
            Self::QueuedByteCount => summary.queued_byte_count,
            Self::MountCount => summary.mount_count,
            Self::TransferredFileCount => summary.transferred_file_count,
            Self::TransferredByteCount => summary.transferred_byte_count,
            Self::TapesCount => summary.tapes_count.unwrap_or_default() as u64,
            Self::TapesFileCount => summary.tapes_file_count,
            Self::TapesByteCount => summary.tapes_byte_count,
            Self::TapesCapacity => summary.tapes_capacity,
            Self::HighestPriority => summary.highest_priority,
        }
    }
}

/// The queues sharing a value of a field, summed up (see [`summarize`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(group_by, group_value)]
pub struct UserRequestSummary {
    /// The field the queues are grouped by
    pub group_by: GroupBy,
    /// Its value in this group
    pub group_value: String,
    /// Lowest minimum request age
    pub min_age: Duration,
    /// Age of the oldest request
    pub oldest_age: Duration,
    /// Files queued
    pub queued_file_count: u64,
    /// Bytes queued
    pub queued_byte_count: u64,
    /// Drives currently mounted
    pub mount_count: u64,
    /// Files transferred by the current mounts
    pub transferred_file_count: u64,
    /// Bytes transferred by the current mounts
    pub transferred_byte_count: u64,
    /// Distinct tapes the requests are for (retrieves only)
    pub tapes_count: Option<usize>,
    /// Files on the tapes of the pools
    pub tapes_file_count: u64,
    /// Bytes on the tapes of the pools
    pub tapes_byte_count: u64,
    /// Capacity of the tapes of the pools
    pub tapes_capacity: u64,
    /// Highest priority
    pub highest_priority: u64,
}

/// The queues given to [`summarize`] are not all of the same [`MountType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the queues to summarize must all be archive queues, or all retrieve queues")]
pub struct MixedMountTypes;

/// Sums up the queues sharing a value of `group_by`, sorted by `order_by` (ascending).
///
/// The queues must all have the same [`MountType`]; the distinct tapes are counted for retrieve queues only.
///
/// # Errors
///
/// [`MixedMountTypes`] when archive and retrieve queues are mixed.
pub fn summarize(
    requests: &[UserRequest],
    group_by: GroupBy,
    order_by: OrderBy,
) -> Result<Vec<UserRequestSummary>, MixedMountTypes> {
    let Some(first) = requests.first() else {
        return Ok(Vec::new());
    };
    if requests.iter().any(|r| r.mount_type != first.mount_type) {
        return Err(MixedMountTypes);
    }
    let mut groups: BTreeMap<&str, Vec<&UserRequest>> = BTreeMap::new();
    for request in requests {
        groups
            .entry(group_by.value(request))
            .or_default()
            .push(request);
    }
    let mut summaries: Vec<_> = groups
        .into_iter()
        .map(|(value, members)| {
            let tapes: HashSet<_> = members.iter().map(|r| r.vid.as_deref()).collect();
            UserRequestSummary {
                group_by,
                group_value: value.to_owned(),
                min_age: members.iter().map(|r| r.min_age).min().unwrap_or_default(),
                oldest_age: members
                    .iter()
                    .map(|r| r.oldest_age)
                    .max()
                    .unwrap_or_default(),
                queued_file_count: members.iter().map(|r| r.queued_file_count).sum(),
                queued_byte_count: members.iter().map(|r| r.queued_byte_count).sum(),
                mount_count: members.iter().map(|r| r.mount_count).sum(),
                transferred_file_count: members.iter().map(|r| r.transferred_file_count).sum(),
                transferred_byte_count: members.iter().map(|r| r.transferred_byte_count).sum(),
                tapes_count: (first.mount_type == MountType::Retrieve).then_some(tapes.len()),
                tapes_file_count: members.iter().map(|r| r.tapes_file_count).sum(),
                tapes_byte_count: members.iter().map(|r| r.tapes_byte_count).sum(),
                tapes_capacity: members.iter().map(|r| r.tapes_capacity).sum(),
                highest_priority: members.iter().map(|r| r.priority).max().unwrap_or_default(),
            }
        })
        .collect();
    summaries.sort_by_key(|summary| order_by.value(summary));
    Ok(summaries)
}
