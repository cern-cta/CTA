// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Requests the scheduler gave up on

use cta_protobuf::cta::admin::{FailedRequestLsItem, FailedRequestLsSummary};
use serde::Serialize;

use crate::{
    ConversionError, Keyed,
    convert::required,
    file::{ArchiveFile, DiskFile, TapeFile},
    scheduler::MountType,
};

/// An archive or retrieve request that failed for good.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(id)]
pub struct FailedRequest {
    /// Identifier of the request in the scheduler (its address in the object store)
    pub id: String,
    /// Direction of the transfer
    pub mount_type: MountType,
    /// User who made the request
    pub requester_name: String,
    /// Group of the user
    pub requester_group: String,
    /// The archived file
    pub archive_file: ArchiveFile,
    /// The disk-side metadata of the file
    pub disk_file: DiskFile,
    /// The tape copy concerned, when the request is for a file on tape
    pub tape_file: Option<TapeFile>,
    /// Tape pool the file is archived to (archive requests)
    pub tape_pool_name: String,
    /// Number of attempts
    pub total_retries: u32,
    /// Error messages of the attempts (`failedrequest ls --log`)
    pub logs: Vec<String>,
}

impl TryFrom<FailedRequestLsItem> for FailedRequest {
    type Error = ConversionError;

    fn try_from(item: FailedRequestLsItem) -> Result<Self, Self::Error> {
        const RECORD: &str = "FailedRequestLsItem";
        let mount_type = MountType::convert_request_type("request_type", item.request_type())?;
        let requester = required(RECORD, "requester", item.requester)?;
        let mut af = required(RECORD, "af", item.af)?;
        let disk_file = DiskFile::from_common(&mut af);
        let copy_number = u32::try_from(item.copy_nb)
            .map_err(|_| ConversionError::invalid_value("copy_nb", item.copy_nb))?;
        Ok(Self {
            id: item.object_id,
            mount_type,
            requester_name: requester.username,
            requester_group: requester.groupname,
            archive_file: ArchiveFile::from_common(af)?,
            disk_file,
            tape_file: item
                .tf
                .filter(|tf| !tf.vid.is_empty())
                .map(|tf| TapeFile::from_common(tf, copy_number)),
            tape_pool_name: item.tapepool,
            total_retries: item.totalretries,
            logs: item.failurelogs,
        })
    }
}

/// The failed requests of one direction, counted (`failedrequest ls --summary`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FailedRequestSummary {
    /// Direction of the transfers
    pub mount_type: MountType,
    /// Number of failed requests
    pub total_file_count: u64,
    /// Bytes of the failed requests
    pub total_byte_count: u64,
    /// Scheduler backend holding the requests
    pub scheduler_backend_name: String,
}

impl TryFrom<FailedRequestLsSummary> for FailedRequestSummary {
    type Error = ConversionError;

    fn try_from(item: FailedRequestLsSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            mount_type: MountType::convert_request_type("request_type", item.request_type())?,
            total_file_count: item.total_files,
            total_byte_count: item.total_size,
            scheduler_backend_name: item.scheduler_backend_name,
        })
    }
}
