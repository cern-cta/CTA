// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Queue status command

use crate::typed_admin_cmd;

/// Shows the status of the active queues (`showqueues`).
#[typed_admin_cmd(Showqueues, response = ShowQueuesItem)]
#[derive(Default)]
pub struct ShowQueues {}
