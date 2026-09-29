/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveCleaner.hpp"

#include <exception>
#include <functional>
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

  enum class AccessMode { ReadOnly, ReadWrite };

  using Mount = std::function<void()>;
  using Cleanup = std::function<DriveCleaner::CleanupResult()>;

  // All borrowed objects, including outcome and callback captures, must outlive the guard.
  MountedTape(mediachanger::MediaChangerFacade& mediaChanger,
              const std::string& vid,
              const mediachanger::LibrarySlot& slot,
              AccessMode accessMode,
              DriveCleaner& cleaner,
              drive::DriveInterface& drive,
              DriveCleaner::DriveStatusReporter reportStatus,
              Outcome& outcome,
              log::LogContext& lc);
  // The mount callback runs synchronously during construction; cleanup is retained for scope exit.
  MountedTape(Mount mount, Cleanup cleanup, Outcome& outcome, log::LogContext& lc);
  ~MountedTape() noexcept;

  MountedTape(const MountedTape&) = delete;
  MountedTape& operator=(const MountedTape&) = delete;
  MountedTape(MountedTape&&) = delete;
  MountedTape& operator=(MountedTape&&) = delete;

  // Finish early when the outcome is needed before scope exit. Repeated calls do nothing.
  // Cleanup is attempted once; failures are logged and retained in the outcome.
  const Outcome& cleanup() noexcept;

private:
  Cleanup m_cleanup;
  Outcome& m_outcome;
  log::LogContext& m_lc;
  bool m_finished = false;
};

}  // namespace cta::tape::daemon
