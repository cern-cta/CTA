/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveOperations.hpp"

namespace cta {
namespace common::dataStructures {
class DriveInfo;
}

namespace log {
class Logger;
}
}  // namespace cta

namespace cta::tape::daemon {
class TapedConfig;

// Build the system operations after the controller has initialized its drive identity.
std::unique_ptr<DriveOperations> makeSystemDriveOperations(const TapedConfig& config,
                                                           log::Logger& log,
                                                           const common::dataStructures::DriveInfo& driveInfo);

}  // namespace cta::tape::daemon
