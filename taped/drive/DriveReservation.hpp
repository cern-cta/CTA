/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::log {
class LogContext;
}

namespace cta::tape::daemon {
/// @brief Track hardware-access ownership for one session lifetime; callers publish lifecycle state.
///
/// Currently tracks logical ownership only; no SCSI reservation is issued.
/// Callers must serialize state changes and establish worker termination before release.
class DriveReservation final {
public:
  /// Start unacquired, borrowing a log context that must outlive this guard.
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

  /// Permit hardware access only while ownership is acquired.
  bool canUseHardware() const noexcept;

  /// @brief Release logical ownership, or confirm that it was already released.
  ///
  /// Release failures propagate; destruction retries as a fallback.
  void release();

private:
  /// Session ownership transitions; released states cannot be reacquired.
  enum class State { Unacquired, Owned, Released };
  State m_state = State::Unacquired;
  log::LogContext& m_lc;
};

}  // namespace cta::tape::daemon
