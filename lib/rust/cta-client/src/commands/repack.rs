// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Repack commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds tapes to repack (`repack add`).
    #[proto(Repack.SubcmdAdd)]
    pub struct Add {
        /// Mount policy of the repack sub-requests
        mount_policy: str => MountPolicy,
        /// A single tape to repack
        vid: str? => Vid,
        /// Several tapes to repack
        vids: str_list? => Vid,
        /// Buffer to use instead of the default repack buffer
        buffer_url: str? => Bufferurl,
        /// Maximum number of files to repack
        max_files_to_select: u64? => MaxFilesToExpand,
        /// Do not mount the tape: only repack the files already in the disk buffer
        no_recall: flag => NoRecall,
        /// Only move the files to new tapes, without creating missing copies
        just_move: flag => Justmove,
        /// Only create the new or missing copies of the files, without moving them
        just_add_copies: flag => Justaddcopies,
    }
}

define_typed_admin_cmd! {
    /// Removes a tape from the tapes to repack (`repack rm`).
    #[proto(Repack.SubcmdRm)]
    pub struct Rm {
        /// Volume identifier of the tape
        vid: str => Vid,
    }
}

define_typed_admin_cmd! {
    /// Lists the repack requests in progress (`repack ls`).
    #[proto(Repack.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the request of this tape
        vid: str? => Vid,
    }
}

define_typed_admin_cmd! {
    /// Shows the repack errors of a tape (`repack err`).
    #[proto(Repack.SubcmdErr)]
    pub struct Err {
        /// Volume identifier of the tape
        vid: str => Vid,
    }
}
