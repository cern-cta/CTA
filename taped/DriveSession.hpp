/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveOperations.hpp"
#include "TapedConfig.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"

#include <memory>
#include <optional>
#include <stop_token>
#include <string>

namespace cta::tape::daemon {

// One period of daemon ownership, containing zero or more tape sessions.
class DriveSession final {
public:
  // All dependencies must outlive the session.
  // Returns null if cleanup fails or the up request is withdrawn; other failures propagate.
  static std::unique_ptr<DriveSession> create(const TapedConfig& config, log::Logger& log, DriveOperations& operations);

  // Run once, until ownership ends or stop is requested. Active transfers are not interrupted.
  void run(std::stop_token stopToken);
  ~DriveSession() noexcept;

  DriveSession(const DriveSession&) = delete;
  DriveSession& operator=(const DriveSession&) = delete;
  DriveSession(DriveSession&&) = delete;
  DriveSession& operator=(DriveSession&&) = delete;

private:
  DriveSession(const TapedConfig& config, log::Logger& log, DriveOperations& operations);

  TapeSessionResult runIteration(std::stop_token stopToken);
  TapeSessionResult runTapeSession(TapeMount& tapeMount);
  // Shared by initial preparation and recovery within an existing ownership period.
  // Preparation uses an unknown VID; only immediate tape-session recovery supplies one.
  bool cleanDrive(const std::optional<std::string>& vid = std::nullopt);
  void release() noexcept;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;
  DriveOperations& m_operations;
};

}  // namespace cta::tape::daemon
