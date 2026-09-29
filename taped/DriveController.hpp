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
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace cta::tape::daemon {

class DriveController final {
public:
  /**
   * @brief Create a controller using caller-owned operations.
   *
   * The configuration, logger and operations must outlive the controller.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @param log Logger used for diagnostics; it must outlive objects retaining a reference to it.
   * @param operations Borrowed external operations that must outlive the controller.
   */
  DriveController(const TapedConfig& config, log::Logger& log, DriveOperations& operations);

  /**
   * @brief Request exit and publish desired-down after registration; active sessions and sleeps are not interrupted.
   */
  void stop();

  /**
   * @brief Register the drive, wait for its library and run scheduling iterations.
   *
   * Registration failures exit without shutdown publication or hardware access.
   * Exceptions escaping the scheduling loop trigger down-state publication without touching tape hardware.
   *
   * @return A nonzero exit code on startup, iteration or shutdown failure.
   */
  int run();

  /**
   * @brief Return the controller liveness result.
   *
   * @return True when the application or controller considers itself live.
   */
  bool isLive() const;

  /**
   * @brief Return whether drive registration has completed.
   *
   * @return True after catalogue registration and scheduler-backend publication succeed.
   */
  bool isReady() const;

private:
  friend class DriveControllerTest;

  // invoked by isLive(); exists to make unit testing easier
  bool isLive(std::chrono::steady_clock::time_point now) const;

  std::stop_source m_stopSource;

  // Read by the health-server thread; registration publishes readiness last.
  std::atomic<bool> m_registered {false};

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  DriveOperations& m_operations;

  // Preserve the cartridge identity across status publications that clear currentVid.
  std::optional<std::string> m_cleanupVid;

  /**
   * @brief Run one scheduling attempt within an already prepared up period.
   *
   * Return a reusable outcome to continue the up period; unsuccessful outcomes request a retry delay.
   * The caller resets the scheduler and applies the delay after the mount has been destroyed.
   * Fatal failures propagate to run().
   */
  TapeSessionResult runIteration();

  /**
   * @brief Execute a session and recover from exceptions once its workers have stopped.
   *
   * The caller keeps the mount and scheduler alive through recovery.
   * @return Session or recovery outcome; incomplete worker teardown remains fatal.
   */
  TapeSessionResult runTapeSession(TapeMount& tapeMount);

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

  /**
   * @brief Publish reported and desired down states, attempting both even if one fails.
   *
   * @param reason Reason category for putting the drive down.
   * @param detail Additional text included in the formatted reason.
   * @param preserveExistingReason Keep an existing operator or failure reason when possible.
   * @throws std::exception The first failed read or publication, after other publications are attempted.
   */
  void putDriveDown(common::dataStructures::DriveDownReason reason,
                    std::string_view detail = {},
                    bool preserveExistingReason = false);

  /**
   * @brief Claim the drive, clean with the preserved VID, then recheck operator intent.
   *
   * Called on entry to an up period or after a session exception with completed worker teardown.
   * @return False if cleaning fails or the operator has requested down.
   */
  bool onDownToUpTransition();
};

}  // namespace cta::tape::daemon
