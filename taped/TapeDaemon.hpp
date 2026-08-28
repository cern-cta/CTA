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

/// @brief Register a drive and run sessions when it is requested Up.
///
/// The desired state requests whether the drive should run; the reported status describes its current activity.
/// Operators and the daemon can request Down. The drive session finishes active work before reporting Down.
class TapeDaemon final {
public:
  /// @brief Create a daemon with borrowed dependencies that must outlive it.
  ///
  /// The scheduler context must use the supplied catalogue.
  TapeDaemon(const TapedConfig& config,
             log::Logger& log,
             catalogue::Catalogue& catalogue,
             SchedulerContext& schedulerContext);

  /// @brief Request exit and, if registered, request the drive Down in the catalogue.
  ///
  /// Safe to call concurrently with run(); catalogue update failures are logged.
  void stop();

  /// @brief Register the drive, wait for its library and run successive drive sessions.
  ///
  /// @return Zero on normal exit, nonzero on failures.
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

  /// @brief Log the exit cause and, if putDriveDown is true, request and report Down in the catalogue.
  ///
  /// @return Zero for normal shutdown, nonzero for an exit or publication failure.
  int shutdown(ExitCause cause, bool putDriveDown, std::string_view exceptionMsg = "");

  /// Coordinates shutdown requests with the run thread.
  std::stop_source m_stopSource;

  /// Ensures the health-server thread can detect readiness.
  std::atomic<bool> m_registered {false};

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  catalogue::Catalogue& m_catalogue;
  SchedulerContext& m_schedulerContext;
  /// Prevents health queries from racing with any active drive session destruction.
  mutable std::mutex m_sessionMutex;
  /// Non-owning; m_sessionMutex protects access while the run thread owns the drive session.
  DriveSession* m_activeDriveSession = nullptr;

  /// @brief Register a fresh drive or retain an interrupted drive for recovery.
  ///
  /// Startup policy determines whether to retain an existing Up request or require the operator to request Up again.
  ///
  /// @return False if the drive name belongs to another host or logical library; catalogue failures throw.
  bool registerDrive();

  /// Poll until the configured logical library exists; database failures propagate.
  void waitForLogicalLibrary();

  /// @brief Wait for the desired state to become Up, reporting Down while waiting.
  ///
  /// If the catalogue entry disappears, throw so run() exits; the next daemon start registers the drive again.
  void waitUntilDriveIsRequestedUp();
};

}  // namespace cta::tape::daemon
