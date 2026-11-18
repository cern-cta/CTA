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
    pub media_type: String,
    /// Vendor of the tape
    pub vendor: String,
    /// Logical library holding the tape
    pub logical_library: String,
    /// Tape pool of the tape
    pub tape_pool: String,
    /// Whether the tape is full
    pub full: bool,
    /// State of the tape
    pub state: Option<TapeState>,
    /// Purchase order of the tape
    #[proto(MediaPurchaseOrderNumber)]
    pub purchase_order: Option<String>,
    /// Reason for the state of the tape
    pub reason: Option<String>,
    /// Comment
    pub comment: Option<String>,
}

/// Changes a tape (`tape ch`).
#[typed_admin_cmd(Tape::Ch)]
pub struct Ch {
    /// Volume identifier of the tape
    pub vid: String,
    /// New media type
    pub media_type: Option<String>,
    /// New vendor
    pub vendor: Option<String>,
    /// New logical library
    pub logical_library: Option<String>,
    /// New tape pool
    pub tape_pool: Option<String>,
    /// New encryption key name
    pub encryption_key_name: Option<String>,
    /// Whether the tape is full
    pub full: Option<bool>,
    /// New verification status
    pub verification_status: Option<String>,
    /// New state
    pub state: Option<TapeState>,
    /// New purchase order
    #[proto(MediaPurchaseOrderNumber)]
    pub purchase_order: Option<String>,
    /// Reason for the new state
    pub reason: Option<String>,
    /// New comment
    pub comment: Option<String>,
    /// Whether the tape is dirty
    pub dirty_bit: Option<bool>,
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
    pub media_type: Option<String>,
    /// Only the tapes of this vendor
    pub vendor: Option<String>,
    /// Only the tapes of this logical library
    pub logical_library: Option<String>,
    /// Only the tapes of this tape pool
    pub tape_pool: Option<String>,
    /// Only the tapes of this virtual organisation
    pub vo: Option<String>,
    /// Only the tapes of this capacity, in bytes
    pub capacity: Option<u64>,
    /// Only the full (`true`) or non-full (`false`) tapes
    pub full: Option<bool>,
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
    pub physical_library: Option<String>,
    /// Only the tapes holding files with missing copies
    #[proto(only_if_true)]
    pub missing_file_copies: bool,
    /// Include the file counts and data size per storage class
    #[proto(GetStorageClassStatistics, only_if_true)]
    pub storage_class_stats: bool,
}
