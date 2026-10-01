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
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>

namespace cta::tape::daemon {
class DriveSession;

// Owns the daemon lifecycle: registration, waiting, successive DriveSessions, and shutdown.
// Each DriveSession is scoped to one iteration of run() and ends before the next begins.
class TapeDaemon final {
public:
  /**
   * @brief Create a daemon and initialize its catalogue and scheduler dependencies.
   *
   * The configuration and logger must outlive the daemon.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @param log Logger used for diagnostics; it must outlive objects retaining a reference to it.
   */
  TapeDaemon(const TapedConfig& config, log::Logger& log);

  // Borrowed dependencies must outlive the daemon. The context must use this catalogue.
  TapeDaemon(const TapedConfig& config,
             log::Logger& log,
             catalogue::Catalogue& catalogue,
             SchedulerContext& schedulerContext);

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

  enum class ExitCause { Normal, RegistrationFailure, MissingDrive, UnexpectedFailure, UnsafeWorkerTeardown };
  enum class DownPublication { None, DesiredOnly, DesiredAndReported };

  // Log the exit and publish only the state owned by the daemon; never access tape hardware.
  int shutdown(ExitCause cause, DownPublication publication, std::string_view diagnostic = "");

  std::stop_source m_stopSource;

  // Read by the health-server thread; registration publishes readiness last.
  std::atomic<bool> m_registered {false};

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  // The scheduler and its backend are destroyed before their catalogue.
  std::unique_ptr<catalogue::Catalogue> m_ownedCatalogue;
  std::unique_ptr<SchedulerContext> m_ownedSchedulerContext;
  catalogue::Catalogue* m_catalogue = nullptr;
  SchedulerContext* m_schedulerContext = nullptr;
  // Borrows the run-thread-owned session only while holding the mutex.
  mutable std::mutex m_sessionMutex;
  // non-owning pointer so that liveness checks can check liveness of the drive session
  DriveSession* m_activeDriveSession = nullptr;

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
