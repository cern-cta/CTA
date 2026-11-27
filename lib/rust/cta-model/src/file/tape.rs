// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The copies of archived files on tape

use cta_protobuf::cta::{admin::tape_file_ls_item, common};
use serde::Serialize;

/// A tape file copy as known to the CTA catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TapeFile {
    /// Volume ID of the tape on which the file has been written
    pub vid: String,
    /// Number of this copy of the file on tape (1..N)
    pub copy_number: u32,
    /// The position of the file on tape: Logical Block ID
    pub block_id: u64,
    /// The position of the file on tape: File Sequence number
    pub fseq: u64,
}

impl From<tape_file_ls_item::TapeFile> for TapeFile {
    fn from(val: tape_file_ls_item::TapeFile) -> Self {
        TapeFile {
            vid: val.vid,
            copy_number: val.copy_nb,
            block_id: val.block_id,
            fseq: val.f_seq,
        }
    }
}

impl TapeFile {
    /// The tape file of a `cta.common.TapeFile`, which does not carry the copy number.
    pub(crate) fn from_common(tf: common::TapeFile, copy_number: u32) -> Self {
        TapeFile {
            vid: tf.vid,
            copy_number,
            block_id: tf.block_id,
            fseq: tf.f_seq,
        }
    }
}
