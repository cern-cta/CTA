/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminCmdMode.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/Scheduler.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace cta::admin_api::commands::sync {

/// @brief What the admin command handlers need from the server and the configuration, for one command
struct SyncCmdContext {
  catalogue::Catalogue& catalogue;
  cta::Scheduler& scheduler;
  /// Already carries the user issuing the command
  log::LogContext& lc;
  /// Identity of the admin issuing the command
  const common::dataStructures::SecurityIdentity& identity;

  /// Name of the scheduler DB, required by the drive commands
  std::optional<std::string> schedulerBackendName;
  /// Which admin commands this admin API serves
  AdminCmdMode adminCommandMode;
  /// Default maximum archived file size (bytes) of a new VO
  uint64_t archiveFileMaxSize;
  /// Default repack buffer URL
  std::optional<std::string> repackBufferURL;
  /// Default maximum number of files a repack expands
  std::optional<uint64_t> repackMaxFilesToSelect;
  /// Minimum quarantine period before a tape can be reclaimed
  uint64_t recycleLogQuarantineSecs;
};

}  // namespace cta::admin_api::commands::sync
