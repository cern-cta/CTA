/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminApiConfig.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::admin_api::commands::sync {

/// @brief What the admin command handlers need from the server and the configuration, for one command
struct SyncCmdContext {
  catalogue::Catalogue& catalogue;
  cta::Scheduler& scheduler;
  /// Already carries the user issuing the command
  log::LogContext& lc;
  /// Identity of the admin issuing the command
  const common::dataStructures::SecurityIdentity& identity;

  /// Fixed rules from the [policies] configuration
  const PoliciesConfig& policies;
  /// Values from the [defaults] configuration, used when a command option is not given
  const DefaultsConfig& defaults;
};

}  // namespace cta::admin_api::commands::sync
