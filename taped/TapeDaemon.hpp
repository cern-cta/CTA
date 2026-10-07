/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "SchedulerContext.hpp"
#include "TapedConfig.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"

#include <atomic>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>

namespace cta::tape::daemon {
class DriveSession;

/// Register one drive, run successive drive sessions and publish shutdown state.
class TapeDaemon final {
public:
  /// @brief Create a daemon with borrowed dependencies that must outlive it.
  ///
  /// The scheduler context must use the supplied catalogue.
  TapeDaemon(const TapedConfig& config,
             log::Logger& log,
             catalogue::Catalogue& catalogue,
             SchedulerContext& schedulerContext);

  /// Request exit and publish desired-down after registration; active sessions and sleeps are not interrupted.
  void stop();

  /// @brief Register the drive, wait for its library and run successive drive sessions.
  ///
  /// @return Zero on normal exit, nonzero on startup, iteration or shutdown failure.
  int run();

  /// @brief Return the active drive session's liveness, or true while no session is active.
  ///
  /// Safe for concurrent health queries.
  bool isLive() const;

  /// @brief Return whether registration succeeded and run() has not yet exited.
  ///
  /// Safe for concurrent health queries.
  bool isReady() const;

private:
  friend class TapeDaemonTest;

  /// Reason for ending the daemon run.
  enum class ExitCause { Normal, RegistrationFailure, MissingDrive, UnexpectedFailure };

  /// Drive-state publication permitted during shutdown.
  enum class DownPublication { None, DesiredAndReported };

  /// @brief Log the exit cause and publish the requested Down state without accessing hardware.
  ///
  /// @return Zero for normal shutdown, nonzero for an exit or publication failure.
  int shutdown(ExitCause cause, DownPublication publication, std::string_view diagnostic = "");

  std::stop_source m_stopSource;

  /// Ensures the health-server thread can detect readiness.
  std::atomic<bool> m_registered {false};

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  catalogue::Catalogue& m_catalogue;
  SchedulerContext& m_schedulerContext;
  /// Prevents health queries from racing with active-session destruction.
  mutable std::mutex m_sessionMutex;
  /// Non-owning; m_sessionMutex protects access while the run thread owns the session.
  DriveSession* m_activeDriveSession = nullptr;

  /// @brief Register a fresh drive or retain an interrupted drive for recovery.
  ///
  /// Reported non-down state with desired-up intent triggers Starting without recreating the entry.
  ///
  /// @param putUpIfPossible Allow ordinary registration to request up when no failure reason prevents it.
  /// @return False for an ownership conflict; catalogue failures propagate.
  bool registerDrive(bool putUpIfPossible);

  /// Poll until the configured logical library exists; database failures propagate.
  void waitForLogicalLibrary();

  /// @brief Poll operator intent, keeping a waiting drive reported down.
  ///
  /// Missing drives propagate to run() and are left for the next daemon run.
  void waitUntilDriveIsRequestedUp();
};

}  // namespace cta::tape::daemon
