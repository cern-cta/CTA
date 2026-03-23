/**
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AdminCmdMode.hpp"

#include "common/exception/Exception.hpp"

namespace cta::common {

AdminCmdMode toAdminCmdMode(const std::string& cmdModeStr) {
  if (cmdModeStr == "all") {
    return AdminCmdMode::ALL;
  }
  if (cmdModeStr == "repack") {
    return AdminCmdMode::REPACK;
  }
  if (cmdModeStr == "norepack") {
    return AdminCmdMode::NO_REPACK;
  }
  if (cmdModeStr == "version") {
    return AdminCmdMode::VERSION;
  }
  if (cmdModeStr == "none") {
    return AdminCmdMode::NONE;
  }
  throw exception::Exception(std::string(cmdModeStr) + " is not a valid admin command mode");
}

}  // namespace cta::common