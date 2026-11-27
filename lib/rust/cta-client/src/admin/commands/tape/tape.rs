// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape commands

use cta_model::tape::TapeState;

use crate::typed_admin_cmd;

/// Adds a tape (`tape add`).
#[typed_admin_cmd(Tape::Add)]
pub struct Add {
    /// Volume identifier of the tape
    pub vid: String,
    /// Media type of the tape
    #[proto(MediaType)]
    pub media_type_name: String,
    /// Vendor of the tape
    pub vendor: String,
    /// Logical library holding the tape
    #[proto(LogicalLibrary)]
    pub logical_library_name: String,
    /// Tape pool of the tape
    #[proto(TapePool)]
    pub tape_pool_name: String,
    /// Whether the tape is full
    #[proto(Full)]
    pub is_full: bool,
    /// State of the tape
    pub state: Option<TapeState>,
    /// Purchase order of the tape
    #[proto(MediaPurchaseOrderNumber)]
    pub purchase_order: Option<String>,
    /// Reason for the state of the tape
    #[proto(Reason)]
    pub state_reason: Option<String>,
    /// Comment
    pub comment: Option<String>,
}

/// Changes a tape (`tape ch`).
#[typed_admin_cmd(Tape::Ch)]
pub struct Ch {
    /// Volume identifier of the tape
    pub vid: String,
    /// New media type
    #[proto(MediaType)]
    pub media_type_name: Option<String>,
    /// New vendor
    pub vendor: Option<String>,
    /// New logical library
    #[proto(LogicalLibrary)]
    pub logical_library_name: Option<String>,
    /// New tape pool
    #[proto(TapePool)]
    pub tape_pool_name: Option<String>,
    /// New encryption key name
    pub encryption_key_name: Option<String>,
    /// Whether the tape is full
    #[proto(Full)]
    pub is_full: Option<bool>,
    /// New verification status
    pub verification_status: Option<String>,
    /// New state
    pub state: Option<TapeState>,
    /// New purchase order
    #[proto(MediaPurchaseOrderNumber)]
    pub purchase_order: Option<String>,
    /// Reason for the new state
    #[proto(Reason)]
    pub state_reason: Option<String>,
    /// New comment
    pub comment: Option<String>,
    /// Whether the tape is dirty
    #[proto(DirtyBit)]
    pub is_dirty: Option<bool>,
}

/// Removes a tape (`tape rm`).
#[typed_admin_cmd(Tape::Rm)]
pub struct Rm {
    /// Volume identifier of the tape
    pub vid: String,
}

/// Reclaims a tape, making it available for writing again (`tape reclaim`).
#[typed_admin_cmd(Tape::Reclaim)]
pub struct Reclaim {
    /// Volume identifier of the tape
    pub vid: String,
}

/// Lists tapes (`tape ls`).
#[typed_admin_cmd(Tape::Ls, response = TapeLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the tape with this volume identifier
    pub vid: Option<String>,
    /// Only the tapes of this media type
    #[proto(MediaType)]
    pub media_type_name: Option<String>,
    /// Only the tapes of this vendor
    pub vendor: Option<String>,
    /// Only the tapes of this logical library
    #[proto(LogicalLibrary)]
    pub logical_library_name: Option<String>,
    /// Only the tapes of this tape pool
    #[proto(TapePool)]
    pub tape_pool_name: Option<String>,
    /// Only the tapes of this virtual organisation
    #[proto(Vo)]
    pub virtual_organisation_name: Option<String>,
    /// Only the tapes of this capacity, in bytes
    pub capacity: Option<u64>,
    /// Only the full (`true`) or non-full (`false`) tapes
    #[proto(Full)]
    pub is_full: Option<bool>,
    /// Only the tapes holding these disk file IDs (decimal)
    #[proto(FileId)]
    pub disk_file_ids: Option<Vec<String>>,
    /// All the tapes, when no other filter is set
    #[proto(only_if_true)]
    pub all: bool,
    /// Only the tapes in this state
    pub state: Option<TapeState>,
    /// Only the tapes imported (`true`) or not (`false`) from CASTOR
    pub from_castor: Option<bool>,
    /// Only the tapes with this purchase order
    #[proto(MediaPurchaseOrderNumber)]
    pub purchase_order: Option<String>,
    /// Only the tapes of this physical library
    #[proto(PhysicalLibrary)]
    pub physical_library_name: Option<String>,
    /// Only the tapes holding files with missing copies
    #[proto(only_if_true)]
    pub missing_file_copies: bool,
    /// Include the file counts and data size per storage class
    #[proto(GetStorageClassStatistics, only_if_true)]
    pub storage_class_stats: bool,
}
