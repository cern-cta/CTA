/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/ExceptionLogging.hpp"

#include <cstdlib>
#include <exception>
#include <string_view>
#include <utility>

namespace cta::tape::daemon {

/// Log the active exception and exit without unwinding when worker termination is uncertain.
[[noreturn]] inline void exitOnUnsafeWorkerFailure(log::LogContext& lc, std::string_view message) noexcept {
  // Even a failure inside a noexcept logger must bypass hardware cleanup and borrowed-state destruction.
  std::set_terminate([] { std::_Exit(EXIT_FAILURE); });
  log::logCurrentExceptionNoThrow(lc, message, log::CRIT);
  std::_Exit(EXIT_FAILURE);
}

/// In some cases if we cannot shutdown workers cleanly, we cannot guarantee that they are not
/// still touching the tape hardware, in which cleaning the drive is unsafe.
/// This function exits without unwinding the stacks (preventing cleaning).
/// The drive is then correctly and safely cleaned/recovered through its normal startup cleanup if configured.
/// Should only happen in very exceptional situations.
template<typename Operation>
void runOrExitOnWorkerFailure(log::LogContext& lc, Operation&& operation) noexcept {
  try {
    std::forward<Operation>(operation)();
  } catch (...) {
    // TODO (graceful shutdown MR): distinguish stopped-worker errors from uncertain termination;
    // currently any escaping startup/join exception forces a restart.
    exitOnUnsafeWorkerFailure(lc,
                              "Tape session worker termination could not be established. Exiting immediately; "
                              "drive recovery is required on restart.");
  }
}

}  // namespace cta::tape::daemon
