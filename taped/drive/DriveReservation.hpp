/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::log {
class LogContext;
}

namespace cta::tape::daemon {
// Owns hardware access for one session lifetime; callers publish lifecycle state.
class DriveReservation final {
public:
  explicit DriveReservation(log::LogContext& lc) noexcept;
  ~DriveReservation() noexcept;

  DriveReservation(const DriveReservation&) = delete;
  DriveReservation& operator=(const DriveReservation&) = delete;
  DriveReservation(DriveReservation&&) = delete;
  DriveReservation& operator=(DriveReservation&&) = delete;

  void acquire();
  // Unsafe ownership is retained but cannot permit further hardware access.
  bool canUseHardware() const noexcept;
  // Return false if worker teardown is unsafe; otherwise release or confirm prior release.
  // Failures propagate; destruction retries as a fallback.
  bool release();
  void markUnsafe() noexcept;

private:
  enum class State { Unacquired, Owned, Released, Unsafe };
  State m_state = State::Unacquired;
  bool m_unsafeReleaseLogged = false;
  log::LogContext& m_lc;
};

}  // namespace cta::tape::daemon
