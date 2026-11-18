// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Workflow events: notifications from disk systems
//!
//! Each event is a struct ([`Create`], [`Archive`], [`Retrieve`], [`CancelRetrieve`], [`Delete`]) describing the
//! file ([`FileInfo`]) and the user ([`Requester`]). [`validate`](Archive::validate) checks what the workflow API
//! would reject before processing the event ([`InvalidEvent`]). Each event is encoded as the protobuf
//! [`Notification`](cta_protobuf::cta::eos::Notification) of the workflow API ([`Event::notification`], validating
//! it first); sending it, to the RPC of its event type, is done by `cta-client`.

use std::collections::BTreeMap;

use crate::file::{Checksum, Owner};

mod notification;

pub use notification::{Event, REQUESTER_INSTANCE};

/// What the events say about the file they are about.
#[derive(Debug, Clone, Default, PartialEq, Eq, bon::Builder)]
pub struct FileInfo {
    /// CTA archive file ID
    pub archive_file_id: Option<u64>,
    /// Disk file ID (for EOS, the file ID in decimal)
    #[builder(into)]
    pub disk_file_id: Option<String>,
    /// Logical path of the file
    #[builder(into)]
    pub path: Option<String>,
    /// Owner of the file
    pub owner: Option<Owner>,
    /// Size in bytes
    pub size: Option<u64>,
    /// Checksums of the file, with their value as in the checksum blobs of the protocol (for Adler-32, the
    /// little-endian bytes)
    #[builder(default)]
    pub checksums: Vec<Checksum>,
    /// Extended attributes
    #[builder(default)]
    pub xattr: BTreeMap<String, String>,
}

impl FileInfo {
    fn validate_archive_file_id(&self) -> Result<(), InvalidEvent> {
        match self.archive_file_id {
            Some(id) if id != 0 => Ok(()),
            _ => Err(InvalidEvent::MissingArchiveFileId),
        }
    }
}

/// The user on whose behalf an event is sent
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requester {
    /// User name
    pub username: String,
    /// Group name
    pub groupname: String,
}

impl Requester {
    /// The user `username` of the group `groupname`.
    pub fn new(username: impl Into<String>, groupname: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            groupname: groupname.into(),
        }
    }
}

/// A file is being created (EOS: `CREATE`): CTA allocates its archive file ID.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder)]
#[builder(on(String, into))]
pub struct Create {
    /// The file
    pub file: FileInfo,
    /// Storage class of the file
    pub storage_class: String,
    /// The user creating the file
    pub requester: Requester,
}

impl Create {
    /// Checks what the workflow API would reject before processing the event: nothing for a `Create`.
    ///
    /// # Errors
    ///
    /// None, kept for symmetry with the other events.
    pub fn validate(&self) -> Result<(), InvalidEvent> {
        Ok(())
    }
}

/// A file has been written and closed (EOS: `CLOSEW`): CTA queues its archival.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder)]
#[builder(on(String, into))]
pub struct Archive {
    /// The file
    pub file: FileInfo,
    /// Storage class of the file (required)
    pub storage_class: String,
    /// The user archiving the file
    pub requester: Requester,
    /// URL of the disk instance to read the file from
    pub service_url: Option<String>,
    /// URL to report the archival to
    pub report_url: Option<String>,
    /// URL to report a failed archival to
    pub error_report_url: Option<String>,
}

impl Archive {
    /// Checks what the workflow API would reject before processing the event.
    ///
    /// # Errors
    ///
    /// [`InvalidEvent::MissingStorageClass`] if the storage class is empty.
    pub fn validate(&self) -> Result<(), InvalidEvent> {
        if self.storage_class.is_empty() {
            return Err(InvalidEvent::MissingStorageClass);
        }
        Ok(())
    }
}

/// A file is to be retrieved from tape (EOS: `PREPARE`).
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder)]
#[builder(on(String, into))]
pub struct Retrieve {
    /// The file (`archive_file_id` required)
    pub file: FileInfo,
    /// The user retrieving the file
    pub requester: Requester,
    /// URL to write the file to
    pub dst_url: String,
    /// Only retrieve from this tape (for files with several tape copies)
    pub vid: Option<String>,
    /// Read the file from tape without writing it to disk
    #[builder(default)]
    pub verify_only: bool,
    /// URL of the disk instance
    pub service_url: Option<String>,
    /// URL to report a failed retrieval to
    pub error_report_url: Option<String>,
}

impl Retrieve {
    /// Checks what the workflow API would reject before processing the event.
    ///
    /// # Errors
    ///
    /// [`InvalidEvent::MissingArchiveFileId`] if the file has no archive file ID (or 0).
    pub fn validate(&self) -> Result<(), InvalidEvent> {
        self.file.validate_archive_file_id()
    }
}

/// A queued retrieval is cancelled (EOS: `ABORT_PREPARE`).
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder)]
#[builder(on(String, into))]
pub struct CancelRetrieve {
    /// The file (`archive_file_id` required)
    pub file: FileInfo,
    /// The user cancelling the retrieval
    pub requester: Requester,
    /// Identifier of the queued retrieve request, as returned when the [`Retrieve`] was sent
    pub request_objectstore_id: String,
}

impl CancelRetrieve {
    /// Checks what the workflow API would reject before processing the event.
    ///
    /// # Errors
    ///
    /// [`InvalidEvent::MissingArchiveFileId`] if the file has no archive file ID (or 0).
    pub fn validate(&self) -> Result<(), InvalidEvent> {
        self.file.validate_archive_file_id()
    }
}

/// A file has been deleted (EOS: `DELETE`).
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder)]
pub struct Delete {
    /// The file (`archive_file_id` required)
    pub file: FileInfo,
    /// The user deleting the file
    pub requester: Requester,
}

impl Delete {
    /// Checks what the workflow API would reject before processing the event.
    ///
    /// # Errors
    ///
    /// [`InvalidEvent::MissingArchiveFileId`] if the file has no archive file ID (or 0).
    pub fn validate(&self) -> Result<(), InvalidEvent> {
        self.file.validate_archive_file_id()
    }
}

/// A workflow event the workflow API would reject before processing it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidEvent {
    /// An `Archive` event without a storage class
    #[error("the storage class is not set")]
    MissingStorageClass,
    /// A `Retrieve`, `CancelRetrieve` or `Delete` event without an archive file ID (or with 0)
    #[error("the archive file ID is not set")]
    MissingArchiveFileId,
}

#[cfg(test)]
mod tests;
