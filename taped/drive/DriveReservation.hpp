/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::log {
class LogContext;
}

namespace cta::tape::daemon {
/// @brief Track whether a drive session may access the hardware.
///
/// This is an in-process ownership flag, not a SCSI reservation or a lock against other processes.
/// The caller updates catalogue status, prevents concurrent state changes and stops all workers before release.
class DriveReservation final {
public:
  /// Start without drive ownership; the supplied log context must outlive this guard.
  explicit DriveReservation(log::LogContext& lc) noexcept;

  /// Attempt release and log any exception without interrupting stack unwinding.
  ~DriveReservation() noexcept;

  /// Copying is prohibited to keep cleanup responsibility unique.
  DriveReservation(const DriveReservation&) = delete;

  /// Copy assignment is prohibited to keep cleanup responsibility unique.
  DriveReservation& operator=(const DriveReservation&) = delete;

  /// Moving is prohibited to keep the guard bound to its session lifetime.
  DriveReservation(DriveReservation&&) = delete;

  /// Move assignment is prohibited to keep the guard bound to its session lifetime.
  DriveReservation& operator=(DriveReservation&&) = delete;

  /// @brief Acquire logical ownership, or keep it if already owned.
  /// @throws std::logic_error If ownership was released.
  void acquire();

  /// Return whether this session currently owns hardware access.
  bool canUseHardware() const noexcept;

  /// End logical ownership, including when never acquired; repeated calls do nothing.
  void release();

private:
  /// Session ownership transitions; released states cannot be reacquired.
  enum class State { Unacquired, Owned, Released };
  State m_state = State::Unacquired;
  log::LogContext& m_lc;
};

}  // namespace cta::tape::daemon
