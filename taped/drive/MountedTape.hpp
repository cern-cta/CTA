/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveCleaner.hpp"
#include "taped/session/VolumeInfo.hpp"

#include <exception>
#include <optional>

namespace cta::tape::daemon {

/// @brief Owns the physical cartridge mount and cleanup within a TapeSession, including a partially failed mount.
/// A scheduler TapeMount is a work assignment; this guard owns the physical mount.
/// Destroy only after all drive users have stopped.
/// Mount failures trigger cleanup before the original exception is rethrown.
class MountedTape {
public:
  /// Cleanup result retained by the caller beyond the guard lifetime.
  struct Outcome {
    std::optional<DriveCleaner::CleanupResult> result;  ///< Absent until cleanup returns normally.
    std::exception_ptr exception;  ///< Preserves a cleanup exception without throwing from the guard.

    /// Require a completed cleanup result whose failure flags permit drive reuse.
    bool driveReusable() const noexcept { return result && result->driveReusable(); }
  };

  /// @brief Mount the cartridge in the volume's access mode and arrange cleanup on scope exit.
  ///
  /// All borrowed objects, including outcome, tracker and reporter captures, must outlive the guard.
  /// Reset outcome before mounting; failed mount attempts trigger cleanup before rethrowing.
  /// @param tapeLoadTimeout Maximum media-readiness wait during cleanup, in seconds.
  MountedTape(mediachanger::MediaChangerFacade& mediaChanger,
              const VolumeInfo& volume,
              drive::DriveInterface& drive,
              catalogue::Catalogue& catalogue,
              uint32_t tapeLoadTimeout,
              DriveCleaner::DriveStatusReporter reportStatus,
              Outcome& outcome,
              log::LogContext& lc,
              TapeSessionTracker& tracker);

  /// Attempt cleanup once after all drive users have stopped, retaining failures in the borrowed outcome.
  ~MountedTape() noexcept;

  /// Copying is prohibited to keep cleanup responsibility unique.
  MountedTape(const MountedTape&) = delete;

  /// Copy assignment is prohibited to keep cleanup responsibility unique.
  MountedTape& operator=(const MountedTape&) = delete;

  /// Moving is prohibited to keep the guard bound to its session lifetime.
  MountedTape(MountedTape&&) = delete;

  /// Move assignment is prohibited to keep the guard bound to its session lifetime.
  MountedTape& operator=(MountedTape&&) = delete;

  /// @brief Finish early when the outcome is needed before scope exit; repeated calls return the same outcome.
  ///
  /// Cleanup is attempted once; failures are logged and retained in the borrowed outcome.
  /// The caller must stop all drive users before cleanup.
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
