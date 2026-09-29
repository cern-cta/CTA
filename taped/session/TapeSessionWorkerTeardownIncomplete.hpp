/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdexcept>

namespace cta::tape::daemon {

// Fatal to the controller: physical cleanup cannot prove that workers have stopped using the drive.
// Throw with std::throw_with_nested to retain the original startup or join failure.
class TapeSessionWorkerTeardownIncomplete : public std::runtime_error {
public:
  TapeSessionWorkerTeardownIncomplete() : std::runtime_error("Tape session worker teardown is incomplete") {}
};

}  // namespace cta::tape::daemon
