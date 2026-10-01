/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::common::dataStructures {
struct DriveInfo;
}

namespace cta::log {
class LogContext;
}

namespace cta::tape::daemon {
class SchedulerContext;

// Owns hardware access and its release publication for one session lifetime.
class HardwareOwnership final {
public:
  HardwareOwnership(const common::dataStructures::DriveInfo& driveInfo,
                    log::LogContext& lc,
                    SchedulerContext& schedulerContext) noexcept;
  ~HardwareOwnership() noexcept;

  HardwareOwnership(const HardwareOwnership&) = delete;
  HardwareOwnership& operator=(const HardwareOwnership&) = delete;
  HardwareOwnership(HardwareOwnership&&) = delete;
  HardwareOwnership& operator=(HardwareOwnership&&) = delete;

  void acquire();
  bool ownsHardware() const noexcept;
  // Explicit release propagates failures; destruction retries as a fallback.
  void release();
  void markUnsafe() noexcept;

private:
  enum class State { Unacquired, Owned, ReleasePending, Released, Unsafe };
  State m_state = State::Unacquired;
  const common::dataStructures::DriveInfo& m_driveInfo;
  log::LogContext& m_lc;
  SchedulerContext& m_schedulerContext;
};

}  // namespace cta::tape::daemon
