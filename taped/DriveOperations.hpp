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
namespace log {
class LogContext;
}
class IScheduler;
class TapeMount;
}  // namespace cta

namespace cta::tape::daemon {

// External operations used by TapeDaemon and DriveSession.
// Keeps catalogue, scheduler, and hardware access replaceable for tests; callers own lifecycle decisions.
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

  /** Retire backend ownership after all mounts and jobs are destroyed; PostgreSQL needs no reset. */
  virtual void resetScheduler() = 0;

  /** Publish desired-down from the stop callback without accessing the replaceable scheduler.
   * Must support concurrent daemon operations; preserve reported state and existing reason/comment.
   */
  virtual void requestDriveDown(log::LogContext& lc) = 0;

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
   * @brief Acquire the next scheduler assignment, or return nullptr when no work is available.
   *
   * @return Owned TapeMount assignment; acquiring it does not physically mount a cartridge.
   */
  virtual std::unique_ptr<TapeMount> getNextMount() = 0;

  /**
   * @brief Run a tape session synchronously for a borrowed scheduler assignment.
   *
   * The production adapter constructs and runs TapeSession within the enclosing DriveSession lifetime.
   * Ordinary exceptions allow DriveSession recovery cleanup; TapeSessionWorkerTeardownIncomplete is fatal.
   * @param mount TapeMount assignment kept alive by DriveSession through execution and recovery.
   * @return Drive reusability and session success, used by DriveSession to decide recovery and scheduling.
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
