/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/ExceptionLogging.hpp"

#include <cstdlib>
#include <exception>
#include <utility>

namespace cta::tape::daemon {

/// In some cases if we cannot shutdown workers cleanly, we cannot guarantee that they are not
/// still touched the tape hardware, in which cleaning the drive is unsafe.
/// This function exits without unwinding the stacks (preventing cleaning).
/// The drive is then correctly and safely cleaned/recovered through its normal startup cleanup.
template<typename Operation>
void runOrExitOnWorkerFailure(log::LogContext& lc, Operation&& operation) noexcept {
  try {
    std::forward<Operation>(operation)();
  } catch (...) {
    // TODO (graceful shutdown MR): distinguish stopped-worker errors from uncertain termination;
    // currently any escaping startup/join exception forces a restart.
    // The logger has noexcept entry points; even a diagnostic failure must use the same immediate exit.
    std::set_terminate([] { std::_Exit(EXIT_FAILURE); });
    log::logCurrentExceptionNoThrow(lc,
                                    "Tape session worker termination could not be established. Exiting immediately; "
                                    "drive recovery is required on restart.",
                                    log::CRIT);
    // Preserve desired state and bypass destructors, exit handlers, and drive cleanup.
    std::_Exit(EXIT_FAILURE);
  }
}

}  // namespace cta::tape::daemon
