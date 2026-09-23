/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/TapeDrive.hpp"
#include "session/TapeSessionResult.hpp"
#include "session/TapeSessionTracker.hpp"

#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace cta {
class IScheduler;
class TapeMount;
}  // namespace cta

namespace cta::tape::daemon {

// External operations needed by the drive lifecycle.
// This is mostly there so that we can nicely implement the unit tests for the DriveController
class DriveOperations {
public:
  /**
   * @brief Release resources owned by the operations implementation.
   */
  virtual ~DriveOperations() = default;

  /**
   * @brief Return the scheduler used for drive state operations.
   *
   * @return Reference to the scheduler used by these operations.
   */
  virtual IScheduler& scheduler() = 0;

  /**
   * @brief Read the existing catalogue entry, or return std::nullopt when the drive is absent.
   *
   * @return Existing catalogue drive entry, or std::nullopt when no entry exists.
   */
  virtual std::optional<common::dataStructures::TapeDrive> getDriveState() = 0;

  /**
   * @brief Check whether the configured logical library exists in the catalogue.
   *
   * @return True if the configured logical library exists.
   */
  virtual bool logicalLibraryExists() = 0;

  /**
   * @brief Check that the drive is empty without changing its contents.
   *
   * @return Empty-drive confirmation and an optional explanation of a failed probe.
   */
  virtual std::pair<bool, std::optional<std::string>> probeDrive() = 0;

  /**
   * @brief Acquire the next scheduled mount, or return nullptr when no work is available.
   *
   * @return Owned mount to execute, or nullptr when no work is available.
   */
  virtual std::unique_ptr<TapeMount> getNextMount() = 0;

  /**
   * @brief Execute a borrowed mount and return the recovery decisions for the controller.
   *
   * @param mount Mount kept alive by the caller until the session returns.
   * @return Drive usability and backend-recovery or retry-delay decisions from the session.
   */
  virtual TapeSessionResult runTapeSession(TapeMount& mount) = 0;

  /** Return only in-memory liveness data, or nullopt when no tape session is active. */
  virtual std::optional<TapeSessionLivenessSnapshot> tapeSessionLiveness() const = 0;

  /**
   * @brief Reset drive configuration and eject any remaining tape.
   *
   * @param vid Cartridge identifier when known; std::nullopt allows cleanup without one.
   * @param waitMediaInDrive Whether to wait for media readiness before cleanup.
   * @return True when cleaning permits reuse of the drive.
   */
  virtual bool clean(const std::optional<std::string>& vid, bool waitMediaInDrive) = 0;

  /**
   * @brief Wait for the requested number of seconds before retrying an operation.
   *
   * @param seconds Requested delay in seconds.
   */
  virtual void sleep(unsigned int seconds) = 0;
};

}  // namespace cta::tape::daemon
