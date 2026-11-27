// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Failed request commands

use crate::typed_admin_cmd;

/// Lists the failed requests (`failedrequest ls`). See [`Summary`] for their totals.
#[typed_admin_cmd(Failedrequest::Ls, response = FailedRequestLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the archive requests
    #[proto(Justarchive, only_if_true)]
    pub just_archive: bool,
    /// Only the retrieve requests
    #[proto(Justretrieve, only_if_true)]
    pub just_retrieve: bool,
    /// Only the requests of this tape pool
    #[proto(TapePool)]
    pub tape_pool_name: Option<String>,
    /// Only the requests of this tape
    pub vid: Option<String>,
    /// Include the log entries of the requests
    #[proto(ShowLogEntries, only_if_true)]
    pub log: bool,
}

/// Shows the totals of all the failed requests (`failedrequest ls --summary`).
///
/// A struct of its own, as `--summary` cannot be combined with the `--tapepool` and `--vid` filters of [`Ls`].
#[typed_admin_cmd(Failedrequest::Ls, response = FailedRequestLsSummary, with(Summary = true))]
#[derive(Default)]
pub struct Summary {
    /// Only the archive requests
    #[proto(Justarchive, only_if_true)]
    pub just_archive: bool,
    /// Only the retrieve requests
    #[proto(Justretrieve, only_if_true)]
    pub just_retrieve: bool,
    /// Include the log entries of the requests
    #[proto(ShowLogEntries, only_if_true)]
    pub log: bool,
}

/// Removes a failed request (`failedrequest rm`).
#[typed_admin_cmd(Failedrequest::Rm)]
pub struct Rm {
    /// Object ID of the request, as shown by `failedrequest ls`
    #[proto(Objectid)]
    pub id: String,
}
