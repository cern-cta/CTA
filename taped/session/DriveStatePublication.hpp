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

// Request desired Down, preserving an observed specific Down reason without reporting hardware release.
void requestDriveDown(catalogue::Catalogue& catalogue,
                      const std::string& driveName,
                      common::dataStructures::DriveDownReason reason,
                      log::LogContext& lc,
                      std::string_view detail = "");

}  // namespace cta::tape::daemon
