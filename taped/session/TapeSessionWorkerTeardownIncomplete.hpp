/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdexcept>

namespace cta::tape::daemon {

/// @brief Signal that worker termination is unproven and the drive must not be reused.
///
/// Throw with std::throw_with_nested to preserve the underlying failure.
class TapeSessionWorkerTeardownIncomplete : public std::runtime_error {
public:
  /// Construct the fatal teardown diagnostic.
  TapeSessionWorkerTeardownIncomplete() : std::runtime_error("Tape session worker teardown is incomplete") {}
};

}  // namespace cta::tape::daemon
