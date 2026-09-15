/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveUsability.hpp"

#include <string>

namespace cta::tape::daemon {

// TODO: would be great if we can simplify this and not require the unknown type
struct TransferSessionResult {
  enum class Outcome { Unknown, NotRequired, Success, Failure };

  // Unknown means the existing session machinery does not establish this outcome independently.
  Outcome transferOutcome = Outcome::Unknown;
  Outcome hardwareCleanupOutcome = Outcome::Unknown;
  Outcome reportingFinalizationOutcome = Outcome::Unknown;
  std::string vid;
  bool loadingAttempted = false;

  // Preserve scheduling behavior separately from the hardware cleanup outcome.
  DriveUsability driveUsability = DriveUsability::Reusable;
};

}  // namespace cta::tape::daemon
