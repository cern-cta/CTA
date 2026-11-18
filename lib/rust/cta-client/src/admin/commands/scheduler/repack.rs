// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Repack commands

use crate::typed_admin_cmd;

/// Adds tapes to repack (`repack add`).
#[typed_admin_cmd(Repack::Add)]
pub struct Add {
    /// Mount policy of the repack sub-requests
    pub mount_policy: String,
    /// A single tape to repack
    pub vid: Option<String>,
    /// Several tapes to repack
    #[proto(Vid)]
    pub vids: Option<Vec<String>>,
    /// Buffer to use instead of the default repack buffer
    #[proto(Bufferurl)]
    pub buffer_url: Option<String>,
    /// Maximum number of files to repack
    #[proto(MaxFilesToExpand)]
    pub max_files_to_select: Option<u64>,
    /// Do not mount the tape: only repack the files already in the disk buffer
    #[proto(only_if_true)]
    pub no_recall: bool,
    /// Only move the files to new tapes, without creating missing copies
    #[proto(Justmove, only_if_true)]
    pub just_move: bool,
    /// Only create the new or missing copies of the files, without moving them
    #[proto(Justaddcopies, only_if_true)]
    pub just_add_copies: bool,
}

/// Removes a tape from the tapes to repack (`repack rm`).
#[typed_admin_cmd(Repack::Rm)]
pub struct Rm {
    /// Volume identifier of the tape
    pub vid: String,
}

/// Lists the repack requests in progress (`repack ls`).
#[typed_admin_cmd(Repack::Ls, response = RepackLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the request of this tape
    pub vid: Option<String>,
}

/// Shows the repack errors of a tape (`repack err`).
#[typed_admin_cmd(Repack::Err)]
pub struct Err {
    /// Volume identifier of the tape
    pub vid: String,
}
