// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a tape (`tape add`).
    #[proto(Tape.SubcmdAdd)]
    pub struct Add {
        /// Volume identifier of the tape
        vid: str => Vid,
        /// Media type of the tape
        media_type: str => MediaType,
        /// Vendor of the tape
        vendor: str => Vendor,
        /// Logical library holding the tape
        logical_library: str => LogicalLibrary,
        /// Tape pool of the tape
        tape_pool: str => TapePool,
        /// Whether the tape is full
        full: bool => Full,
        /// State of the tape
        state: str? => State,
        /// Purchase order of the tape
        purchase_order: str? => MediaPurchaseOrderNumber,
        /// Reason for the state of the tape
        reason: str? => Reason,
        /// Comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a tape (`tape ch`).
    #[proto(Tape.SubcmdCh)]
    pub struct Ch {
        /// Volume identifier of the tape
        vid: str => Vid,
        /// New media type
        media_type: str? => MediaType,
        /// New vendor
        vendor: str? => Vendor,
        /// New logical library
        logical_library: str? => LogicalLibrary,
        /// New tape pool
        tape_pool: str? => TapePool,
        /// New encryption key name
        encryption_key_name: str? => EncryptionKeyName,
        /// Whether the tape is full
        full: bool? => Full,
        /// New verification status
        verification_status: str? => VerificationStatus,
        /// New state
        state: str? => State,
        /// New purchase order
        purchase_order: str? => MediaPurchaseOrderNumber,
        /// Reason for the new state
        reason: str? => Reason,
        /// New comment
        comment: str? => Comment,
        /// Whether the tape is dirty
        dirty_bit: bool? => DirtyBit,
    }
}

define_typed_admin_cmd! {
    /// Removes a tape (`tape rm`).
    #[proto(Tape.SubcmdRm)]
    pub struct Rm {
        /// Volume identifier of the tape
        vid: str => Vid,
    }
}

define_typed_admin_cmd! {
    /// Reclaims a tape, making it available for writing again (`tape reclaim`).
    #[proto(Tape.SubcmdReclaim)]
    pub struct Reclaim {
        /// Volume identifier of the tape
        vid: str => Vid,
    }
}

define_typed_admin_cmd! {
    /// Lists tapes (`tape ls`).
    #[proto(Tape.SubcmdLs)]
    #[item(TalsItem: TapeLsItem)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the tape with this volume identifier
        vid: str? => Vid,
        /// Only the tapes of this media type
        media_type: str? => MediaType,
        /// Only the tapes of this vendor
        vendor: str? => Vendor,
        /// Only the tapes of this logical library
        logical_library: str? => LogicalLibrary,
        /// Only the tapes of this tape pool
        tape_pool: str? => TapePool,
        /// Only the tapes of this virtual organisation
        vo: str? => Vo,
        /// Only the tapes of this capacity, in bytes
        capacity: u64? => Capacity,
        /// Only the full (`true`) or non-full (`false`) tapes
        full: bool? => Full,
        /// Only the tapes holding these disk file IDs (decimal)
        disk_file_ids: str_list? => FileId,
        /// All the tapes, when no other filter is set
        all: flag => All,
        /// Only the tapes in this state
        state: str? => State,
        /// Only the tapes imported (`true`) or not (`false`) from CASTOR
        from_castor: bool? => FromCastor,
        /// Only the tapes with this purchase order
        purchase_order: str? => MediaPurchaseOrderNumber,
        /// Only the tapes of this physical library
        physical_library: str? => PhysicalLibrary,
        /// Only the tapes holding files with missing copies
        missing_file_copies: flag => MissingFileCopies,
        /// Include the file counts and data size per storage class
        storage_class_stats: flag => GetStorageClassStatistics,
    }
}
