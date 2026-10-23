/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminCmdMode.hpp"
#include "commands/stream/CtaAdminResponseStream.hpp"

#include <catalogue/Catalogue.hpp>
#include <cstdint>
#include <memory>
#include <scheduler/Scheduler.hpp>
#include <string>

#include "cta_admin.pb.h"

namespace cta::log {
class LogContext;
}

namespace cta::admin_api::commands::stream {

/// @brief What the streams may need from the server to be built
///
/// The references are not owned: they must outlive the streams built from this context.
struct StreamContext {
  /// Source of the catalogue entities (tapes, pools, users, ...) to stream
  cta::catalogue::Catalogue& catalogue;
  /// Source of the scheduler state (queues, drives, ...) to stream
  cta::Scheduler& scheduler;
  /// Scheduler database backing the scheduler
  cta::SchedulerDatabase& schedDb;
  /// Log context used by the streams
  cta::log::LogContext& lc;
  /// CTA instance name, set on each streamed item
  const std::string& instanceName;
  /// Reported by the version command
  const std::string& catalogueConnString;
  /// Minimum age in seconds of a file missing copies to be reported by tape ls
  uint64_t missingFileCopiesMinAgeSecs;
  /// Which commands this admin API serves, see `AdminCmdMode::isCommandDisabled()`
  AdminCmdMode adminCommandMode;
};

/// @brief Builds the stream serving a streamed admin command
///
/// @param context   What the streams need from the server (see `StreamContext`)
/// @param adminCmd  The admin command, whose options select and filter the items to stream
///
/// @return The stream, which also carries the header to send before its items, or nullptr if the command (cmd, subcmd
/// pair) is not a streamed command
/// @throws cta::exception::UserError if the command is rejected by the mode of the admin API, or if its options are
/// invalid
std::unique_ptr<CtaAdminResponseStream> createStream(const StreamContext& context, const admin::AdminCmd& adminCmd);

}  // namespace cta::admin_api::commands::stream
