/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "HardwareOwnership.hpp"
#include "TapedConfig.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "session/TapeSessionResult.hpp"
#include "session/TapeSessionTracker.hpp"
#include "system/Wrapper.hpp"

#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace cta {
class TapeMount;
}  // namespace cta

namespace cta::tape::daemon {
class SchedulerContext;

// One period of hardware ownership within TapeDaemon, containing zero or more tape sessions.
class DriveSession final {
public:
  // Borrowed dependencies must outlive the session; hardware access objects are session-owned.
  // Performs initial drive preparation before returning a session ready to run.
  // Returns null if cleanup fails or the up request is withdrawn; other failures propagate.
  static std::unique_ptr<DriveSession>
  create(const TapedConfig& config, log::Logger& log, SchedulerContext& schedulerContext);

  // Run once, until ownership ends or stop is requested. Active transfers are not interrupted.
  void run(std::stop_token stopToken);
  ~DriveSession() noexcept;

  // Safe for concurrent health queries; idle periods have no active tape session.
  bool isLive() const;

  DriveSession(const DriveSession&) = delete;
  DriveSession& operator=(const DriveSession&) = delete;
  DriveSession(DriveSession&&) = delete;
  DriveSession& operator=(DriveSession&&) = delete;

private:
  // Initialize dependencies; create() prepares the drive under unique ownership.
  DriveSession(const TapedConfig& config, log::Logger& log, SchedulerContext& schedulerContext);

  TapeSessionResult runIteration(std::stop_token stopToken);
  TapeSessionResult runTapeSession(TapeMount& tapeMount);
  // Shared by initial preparation and recovery within an existing ownership period.
  // Preparation uses an unknown VID; only immediate tape-session recovery supplies one.
  bool cleanDrive(const std::optional<std::string>& vid = std::nullopt);
  void requestDown(common::dataStructures::DriveDownReason reason, std::string_view detail = "");
  void requestDownNoThrow(common::dataStructures::DriveDownReason reason, std::string_view detail) noexcept;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;
  SchedulerContext& m_schedulerContext;
  // Hardware access is scoped to this ownership period, including preparation and recovery.
  mediachanger::MediaChangerFacade m_mediaChanger;
  System::realWrapper m_sysWrapper;
  // Atomically published by the run thread; health readers retain their own shared reference.
  std::shared_ptr<const TapeSessionTracker> m_activeTracker;
  // Destroy first, while its borrowed dependencies and hardware access objects are still alive.
  HardwareOwnership m_hardwareOwnership;
};

}  // namespace cta::tape::daemon
