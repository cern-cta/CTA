// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Failed request commands

use bon::Builder;
use cta_protobuf::cta::admin::{
    AdminCmd,
    admin_cmd::{Cmd, SubCmd},
};

use crate::{admin_cmd, commands::TypedAdminCmd, define_typed_admin_cmd};

define_typed_admin_cmd! {
    /// Lists the failed requests (`failedrequest ls`). See [`Summary`] for their totals.
    #[proto(Failedrequest.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {
        /// Only the archive requests
        just_archive: flag => Justarchive,
        /// Only the retrieve requests
        just_retrieve: flag => Justretrieve,
        /// Only the requests of this tape pool
        tape_pool: str? => TapePool,
        /// Only the requests of this tape
        vid: str? => Vid,
        /// Include the log entries of the requests
        log: flag => ShowLogEntries,
    }
}

/// Shows the totals of all the failed requests (`failedrequest ls --summary`).
///
/// A struct of its own, as `--summary` cannot be combined with the `--tapepool` and `--vid` filters of [`Ls`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Builder)]
pub struct Summary {
    /// Only the archive requests
    #[builder(default)]
    pub just_archive: bool,
    /// Only the retrieve requests
    #[builder(default)]
    pub just_retrieve: bool,
    /// Include the log entries of the requests
    #[builder(default)]
    pub log: bool,
}

impl From<Summary> for AdminCmd {
    fn from(value: Summary) -> Self {
        admin_cmd!(Failedrequest.SubcmdLs {
            Justarchive: flag => value.just_archive,
            Justretrieve: flag => value.just_retrieve,
            ShowLogEntries: flag => value.log,
            Summary: flag => true,
        })
    }
}

impl TypedAdminCmd for Summary {
    const CMD: Cmd = Cmd::Failedrequest;
    const SUBCMD: SubCmd = SubCmd::SubcmdLs;
}

define_typed_admin_cmd! {
    /// Removes a failed request (`failedrequest rm`).
    #[proto(Failedrequest.SubcmdRm)]
    pub struct Rm {
        /// Object ID of the request, as shown by `failedrequest ls`
        object_id: str => Objectid,
    }
}
