/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveCleaner.hpp"
#include "taped/session/VolumeInfo.hpp"

#include <optional>

namespace cta::tape::daemon {

/// @brief Owns the physical cartridge mount and cleanup within a TapeSession, including a partially failed mount.
/// Destroy only after all drive users have stopped.
class MountedTape {
public:
  /// @brief Mount the cartridge in the volume's access mode and arrange cleanup on scope exit.
  ///
  /// All borrowed objects, including cleanup result and tracker, must outlive the guard.
  /// Reset cleanup result before mounting; failed mount attempts trigger cleanup before rethrowing.
  /// @param tapeLoadTimeout Maximum media-readiness wait during cleanup, in seconds.
  MountedTape(mediachanger::MediaChangerFacade& mediaChanger,
              const VolumeInfo& volume,
              drive::DriveInterface& drive,
              catalogue::Catalogue& catalogue,
              uint32_t tapeLoadTimeout,
              std::optional<DriveCleaner::CleanupResult>& cleanupResult,
              log::LogContext& lc,
              TapeSessionTracker& tracker);

  /// Attempt cleanup once after all drive users have stopped, retaining failures in the borrowed cleanup result.
  ~MountedTape() noexcept;

  /// Copying is prohibited to keep cleanup responsibility unique.
  MountedTape(const MountedTape&) = delete;

  /// Copy assignment is prohibited to keep cleanup responsibility unique.
  MountedTape& operator=(const MountedTape&) = delete;

  /// Moving is prohibited to keep the guard bound to its session lifetime.
  MountedTape(MountedTape&&) = delete;

  /// Move assignment is prohibited to keep the guard bound to its session lifetime.
  MountedTape& operator=(MountedTape&&) = delete;

  /// @brief Finish early when the cleanup result is needed before scope exit; repeated calls return the same cleanup result.
  ///
  /// Cleanup is attempted once; failures are logged and retained in the borrowed cleanup result.
  /// The caller must stop all drive users before cleanup.
  const DriveCleaner::CleanupResult& cleanup() noexcept;

private:
  DriveCleaner m_cleaner;
  drive::DriveInterface& m_drive;
  std::optional<DriveCleaner::CleanupResult>& m_cleanupResult;
  log::LogContext& m_lc;
  bool m_finished = false;
};

}  // namespace cta::tape::daemon
