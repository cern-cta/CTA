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
#include <string_view>

namespace cta::tape::daemon {

class DriveController final {
public:
  DriveController(const TapedConfig& config, log::Logger& log);

  // The config and operations must outlive a controller constructed with borrowed operations.
  DriveController(const TapedConfig& config, log::Logger& log, DriveOperations& operations);

  ~DriveController();

  void stop();

  int run();

  bool isLive() const;

  bool isReady() const;

private:
  friend class DriveControllerTest;

  void runIteration();

  // Helpers recover only from expected local conditions. Operational failures propagate to run().
  // Registration returns false for an ownership conflict and creates an entry if one is absent.
  bool registerDrive(bool putUpIfPossible);
  // Wait for the configured logical library to exist before scheduling at startup.
  void waitForLogicalLibrary();
  // Poll until the catalogue and scheduler backend are reachable again.
  void waitForBackendRecovery();
  // Register a missing drive as down while waiting for the operator's up request.
  void waitUntilDriveIsRequestedUp();
  // Attempt both publications and propagate the first failure after logging each failed operation.
  void putDriveDown(common::dataStructures::DriveDownReason reason,
                    std::string_view detail = {},
                    bool preserveExistingReason = false);
  // Return false when probing requests down and scheduling must be skipped.
  bool prepareDriveForScheduling();
  // Convert cleaner exceptions to a failed cleaning result and log the caller's context.
  // Clean and publish down, returning a nonzero exit code if either operation fails.
  int shutdownDrive();

  std::stop_source m_stopSource;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  // Destroy the owned operations before the drive information it borrows.
  std::unique_ptr<DriveOperations> m_ownedOperations;
  DriveOperations& m_operations;
};

}  // namespace cta::tape::daemon
