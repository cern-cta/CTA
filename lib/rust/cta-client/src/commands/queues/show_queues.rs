// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Queue status command

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Shows the status of the active queues (`showqueues`).
    #[proto(Showqueues.SubcmdNone)]
    #[item(SqItem: ShowQueuesItem)]
    #[derive(Default)]
    pub struct ShowQueues {}
}
