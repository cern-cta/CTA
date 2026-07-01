/**
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/config/Config.hpp"

#include <optional>
#include <string>

namespace cta::frontend {

enum class OperationMode { WFE, ADMIN_ALL, ADMIN_REPACK, ADMIN_NO_REPACK, ADMIN_VERSION };

/**
 * @brief Convert an OperationMode to a string
 */
std::string toString(OperationMode mode);

/**
 * @brief Enum representing all possible states of the Admin API
 */
enum class AdminCmdMode {
  ALL,        //!< All admin commands are allowed
  REPACK,     //!< Only repack (and version) admin commands are allowed
  NO_REPACK,  //!< No repack admin commands are allowed; all other commands allowed
  VERSION,    //!< Only version admin command is allowed
};

/**
 * @brief Convert an AdminCmdMode to a string
 */
std::string toString(AdminCmdMode mode);

/**
 * @brief Converts an OperationMode into an AdminCmdMode. This will work fine unless
 * the OperationMode is WFE (exception)
 */
AdminCmdMode toAdminCmdMode(OperationMode mode);
}  // namespace cta::frontend

template<>
struct cta::common::FromString<cta::frontend::OperationMode> {
  static std::optional<cta::frontend::OperationMode> tryFrom(std::string_view text) {
    using enum cta::frontend::OperationMode;
    if (text == "wfe") {
      return WFE;
    } else if (text == "admin_all") {
      return ADMIN_ALL;
    } else if (text == "admin_repack") {
      return ADMIN_REPACK;
    } else if (text == "admin_no_repack") {
      return ADMIN_NO_REPACK;
    } else if (text == "admin_version") {
      return ADMIN_VERSION;
    } else {
      return std::nullopt;
    }
  }
};
