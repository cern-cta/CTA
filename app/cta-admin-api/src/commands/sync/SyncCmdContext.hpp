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

/**
 * @brief What the admin command handlers need from the server and the configuration, for one command
 */
struct SyncCmdContext {
  catalogue::Catalogue& catalogue;
  cta::Scheduler& scheduler;
  log::LogContext& lc;                                       //!< Already carries the user issuing the command
  const common::dataStructures::SecurityIdentity& identity;  //!< Identity of the admin issuing the command

  std::optional<std::string> schedulerBackendName;  //!< Name of the scheduler DB, required by the drive commands
  AdminCmdMode adminCommandMode;                    //!< Which admin commands this admin API serves
  uint64_t archiveFileMaxSize;                      //!< Default maximum archived file size (bytes) of a new VO
  std::optional<std::string> repackBufferURL;       //!< Default repack buffer URL
  std::optional<uint64_t> repackMaxFilesToSelect;   //!< Default maximum number of files a repack expands
  uint64_t recycleLogQuarantineSecs;                //!< Minimum quarantine period before a tape can be reclaimed
};

}  // namespace cta::admin_api::commands::sync
