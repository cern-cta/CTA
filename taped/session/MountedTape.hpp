/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveCleaner.hpp"
#include "VolumeInfo.hpp"

#include <exception>
#include <optional>

namespace cta::tape::daemon {

/** Mounts a cartridge and owns its cleanup, including a partially failed mount.
 * Destroy only after all drive users have stopped.
 * Mount failures trigger cleanup before the original exception is rethrown.
 * This guard does not coordinate workers or publish the final drive status.
 */
class MountedTape {
public:
  // Keep this outside the guard's scope to inspect the final cleanup outcome.
  struct Outcome {
    std::optional<DriveCleaner::CleanupResult> result;
    std::exception_ptr exception;

    bool driveReusable() const noexcept { return result && result->driveReusable(); }
  };

  // All borrowed objects, including outcome, tracker, and reporter captures, must outlive the guard.
  MountedTape(mediachanger::MediaChangerFacade& mediaChanger,
              const VolumeInfo& volume,
              drive::DriveInterface& drive,
              catalogue::Catalogue& catalogue,
              uint32_t tapeLoadTimeout,
              DriveCleaner::DriveStatusReporter reportStatus,
              Outcome& outcome,
              log::LogContext& lc,
              TapeSessionTracker& tracker);
  ~MountedTape() noexcept;

  MountedTape(const MountedTape&) = delete;
  MountedTape& operator=(const MountedTape&) = delete;
  MountedTape(MountedTape&&) = delete;
  MountedTape& operator=(MountedTape&&) = delete;

  // Finish early when the outcome is needed before scope exit. Repeated calls do nothing.
  // Cleanup is attempted once; failures are logged and retained in the outcome.
  const Outcome& cleanup() noexcept;

private:
  DriveCleaner m_cleaner;
  drive::DriveInterface& m_drive;
  DriveCleaner::DriveStatusReporter m_reportStatus;
  Outcome& m_outcome;
  log::LogContext& m_lc;
  bool m_finished = false;
};

}  // namespace cta::tape::daemon
