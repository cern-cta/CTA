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

#include <atomic>
#include <chrono>
#include <memory>
#include <stop_token>
#include <string>

namespace cta::tape::daemon {

// Owns the daemon lifecycle: registration, waiting, successive DriveSessions, and shutdown.
// Each DriveSession is scoped to one iteration of run() and ends before the next begins.
class TapeDaemon final {
public:
  /**
   * @brief Create a daemon using caller-owned operations.
   *
   * The configuration, logger and operations must outlive the daemon.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @param log Logger used for diagnostics; it must outlive objects retaining a reference to it.
   * @param operations Borrowed external operations that must outlive the daemon.
   */
  TapeDaemon(const TapedConfig& config, log::Logger& log, DriveOperations& operations);

  /**
   * @brief Request exit and publish desired-down after registration; active sessions and sleeps are not interrupted.
   */
  void stop();

  /**
   * @brief Register the drive, wait for its library and run successive drive sessions.
   *
   * Registration failures exit without shutdown publication or hardware access.
   * Exceptions escaping the scheduling loop trigger down-state publication without touching tape hardware.
   *
   * @return A nonzero exit code on startup, iteration or shutdown failure.
   */
  int run();

  /**
   * @brief Return the daemon liveness result.
   *
   * @return True when the application or daemon considers itself live.
   */
  bool isLive() const;

  /**
   * @brief Return whether drive registration has completed.
   *
   * @return True after catalogue registration and scheduler-backend publication succeed.
   */
  bool isReady() const;

private:
  friend class TapeDaemonTest;

  // invoked by isLive(); exists to make unit testing easier
  bool isLive(std::chrono::steady_clock::time_point now) const;

  std::stop_source m_stopSource;

  // Read by the health-server thread; registration publishes readiness last.
  std::atomic<bool> m_registered {false};

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  DriveOperations& m_operations;

  /**
   * @brief Register a fresh drive or retain an interrupted drive for recovery.
   *
   * Reported non-down state with desired-up intent triggers CleaningUp without recreating the entry.
   *
   * @param putUpIfPossible Allow ordinary registration to request up when no failure reason prevents it.
   * @return False for an ownership conflict; catalogue failures propagate.
   */
  bool registerDrive(bool putUpIfPossible);

  /**
   * @brief Poll until the configured logical library exists; database failures propagate.
   */
  void waitForLogicalLibrary();

  /**
   * @brief Poll operator intent, keeping a waiting drive reported down.
   *
   * Missing drives propagate to run() and are left for the next daemon run.
   */
  void waitUntilDriveIsRequestedUp();
};

}  // namespace cta::tape::daemon
