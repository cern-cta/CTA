/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveDownReason.hpp"

#include <string>
#include <string_view>

namespace cta::catalogue {
class Catalogue;
}

namespace cta::log {
class LogContext;
}

namespace cta::tape::daemon {

/// @brief Request desired Down, preserving an observed specific Down reason.
///
/// Does not report hardware release. Catalogue failures propagate.
/// @param catalogue Catalogue containing the drive.
/// @param driveName Drive to update.
/// @param reason Reason used unless an existing specific reason is preserved.
/// @param lc Logging context for publication.
/// @param detail Optional diagnostic appended to the reason.
void requestDriveDown(catalogue::Catalogue& catalogue,
                      const std::string& driveName,
                      common::dataStructures::DriveDownReason reason,
                      log::LogContext& lc,
                      std::string_view detail = "");

}  // namespace cta::tape::daemon
