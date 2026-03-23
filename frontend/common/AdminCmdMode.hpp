/**
* SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::common {

enum class AdminCmdMode {
  ALL,        //!< All admin commands are allowed
  REPACK,     //!< Only repack (and version) admin commands are allowed
  NO_REPACK,  //!< No repack admin commands are allowed; all other commands allowed
  VERSION,    //!< Only version admin command is allowed
  NONE,       //!< No admin commands allowed
};

AdminCmdMode toAdminCmdMode(const std::string& cmdModeStr);

}  // namespace cta::common