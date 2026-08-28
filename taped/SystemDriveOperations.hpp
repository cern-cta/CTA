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

/**
 * @brief Build catalogue, scheduler and hardware operations for a drive.
 *
 * The configuration, logger and drive identity must outlive the returned operations.
 * Initialization failures propagate to the caller.
 *
 * @param config Daemon configuration; borrowed configuration must outlive the owning object.
 * @param log Logger used for diagnostics; it must outlive objects retaining a reference to it.
 * @param driveInfo Drive identity and device paths; retained references must remain valid for the object lifetime.
 * @return Owned system operations for the configured drive.
 */
std::unique_ptr<DriveOperations> makeSystemDriveOperations(const TapedConfig& config,
                                                           log::Logger& log,
                                                           const common::dataStructures::DriveInfo& driveInfo);

}  // namespace cta::tape::daemon
