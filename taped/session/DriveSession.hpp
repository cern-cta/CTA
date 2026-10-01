/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeSessionResult.hpp"
#include "TapeSessionTracker.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/TapedConfig.hpp"
#include "taped/drive/HardwareOwnership.hpp"
#include "taped/system/Wrapper.hpp"

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
  // Constructs dependencies without accessing the drive; run() performs initial preparation.
  static std::unique_ptr<DriveSession>
  create(const TapedConfig& config, log::Logger& log, SchedulerContext& schedulerContext);

  // The supplied system wrapper must outlive the session.
  static std::unique_ptr<DriveSession> create(const TapedConfig& config,
                                              log::Logger& log,
                                              SchedulerContext& schedulerContext,
                                              System::virtualWrapper& sysWrapper);

  // Prepare and run once, until ownership ends or stop is requested. Active transfers are not interrupted.
  // Preparation failure or a withdrawn up request ends the session without scheduling; other failures propagate.
  void run(std::stop_token stopToken);
  ~DriveSession() noexcept;

  // Safe for concurrent health queries; preparation, recovery, and transfers publish their trackers.
  bool isLive() const;

  DriveSession(const DriveSession&) = delete;
  DriveSession& operator=(const DriveSession&) = delete;
  DriveSession(DriveSession&&) = delete;
  DriveSession& operator=(DriveSession&&) = delete;

private:
  friend class DriveSessionLivenessTest;
  friend class DriveSessionTest;

  // Initialize dependencies; run() prepares the drive under unique ownership.
  DriveSession(const TapedConfig& config,
               log::Logger& log,
               SchedulerContext& schedulerContext,
               System::virtualWrapper* sysWrapper = nullptr);

  TapeSessionResult runIteration(std::stop_token stopToken);
  TapeSessionResult runTapeSession(TapeMount& tapeMount);
  // Shared by initial preparation and recovery within an existing ownership period.
  // Preparation uses an unknown VID; only immediate tape-session recovery supplies one.
  bool cleanDrive(const std::optional<std::string>& vid = std::nullopt);
  // Release hardware before terminal publication; destruction retries failed publication.
  void releaseAndReportDown();
  void requestDown(common::dataStructures::DriveDownReason reason, std::string_view detail = "");
  void requestDownNoThrow(common::dataStructures::DriveDownReason reason, std::string_view detail) noexcept;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;
  SchedulerContext& m_schedulerContext;
  // Hardware access is scoped to this ownership period, including preparation and recovery.
  mediachanger::MediaChangerFacade m_mediaChanger;
  System::realWrapper m_realSysWrapper;
  System::virtualWrapper& m_sysWrapper;
  // Active cleanup or transfer tracker; health readers retain their own atomically loaded shared reference.
  std::shared_ptr<const TapeSessionTracker> m_activeTracker;
  // Tracks terminal publication only; hardware ownership is managed independently.
  bool m_downReported = false;
  // Destroy first, while its borrowed dependencies and hardware access objects are still alive.
  HardwareOwnership m_hardwareOwnership;
};

}  // namespace cta::tape::daemon
