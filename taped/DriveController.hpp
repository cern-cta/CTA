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
#include <stop_token>
#include <string>
#include <string_view>

namespace cta::tape::daemon {

class DriveController final {
public:
  /**
   * @brief Create a controller with system operations for the configured drive.
   *
   * The configuration and logger must outlive the controller.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @param log Logger used for diagnostics; it must outlive objects retaining a reference to it.
   */
  DriveController(const TapedConfig& config, log::Logger& log);

  /**
   * @brief Create a controller using borrowed operations. Useful for unit tests.
   *
   * The configuration, logger and operations must outlive the controller.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @param log Logger used for diagnostics; it must outlive objects retaining a reference to it.
   * @param operations Borrowed external operations that must outlive the controller.
   */
  DriveController(const TapedConfig& config, log::Logger& log, DriveOperations& operations);

  /**
   * @brief Release owned operations before the drive identity they reference.
   */
  ~DriveController();

  /**
   * @brief Gracefully stop the drive controller (including any active tape sessions).
   */
  void stop();

  /**
   * @brief Register the drive, wait for its library and run scheduling iterations.
   *
   * Startup failures exit without touching hardware.
   * Exceptions escaping the scheduling loop trigger final cleanup and down-state publication.
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
   * @brief Return the controller readiness result.
   *
   * @return True when the application or controller considers itself ready.
   */
  bool isReady() const;

private:
  friend class DriveControllerTest;

  std::stop_source m_stopSource;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  // Destroy the owned operations before the drive information it borrows.
  std::unique_ptr<DriveOperations> m_ownedOperations;
  DriveOperations& m_operations;

  // Arm once per observed down period, explicit controller down, or registration.
  bool m_cleanBeforeScheduling = true;

  /**
   * @brief Prepare the drive, acquire and execute a mount, and apply recovery decisions.
   *
   * Wait before retrying idle or recoverable scheduling failures.
   * Unrecoverable failures propagate to run().
   */
  void runIteration();

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
   * @brief Retry scheduler health checks while database connections remain unavailable.
   */
  void waitForBackendRecovery();

  /**
   * @brief Poll operator intent, keeping a waiting drive reported down.
   *
   * Register a missing drive as down and arm cleanup for the next up request.
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
   * @brief Wait for up intent, clean when needed and probe before advertising an idle drive.
   *
   * @return False when cleaning or probing prevents scheduling.
   */
  bool prepareDriveForScheduling();

  /**
   * @brief Clean with an unknown VID, then recheck operator intent.
   *
   * @return False if cleaning fails or the operator has requested down.
   */
  bool cleanBeforeScheduling();

  /**
   * @brief Attempt final cleanup and publish down while preserving an existing failure reason.
   *
   * @return Zero on success, or a nonzero exit code when cleaning or publication fails.
   */
  int shutdownDrive();
};

}  // namespace cta::tape::daemon
