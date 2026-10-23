/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/config/Config.hpp"

#include <optional>
#include <string_view>

#include "cta_admin.pb.h"

namespace cta::admin_api {

/// @brief Role of the admin API with respect to the repacks, meant to match the scheduler DB it is connected to when
/// the user and the repack requests are served from separate ones
///
/// The mode only concerns the repack commands and the tape state changes: it is not a general filter of the admin
/// commands. See isCommandDisabled() for what it rejects.
enum class AdminCmdMode {
  /// No restriction: for a scheduler DB serving both the user and the repack requests
  ALL,
  /// For a repack-only scheduler DB. No command is rejected. The tape state changes (tape ch --state) follow the
  /// rules of a repack-only scheduler DB (Scheduler::OperatingMode::REPACK).
  REPACK,
  /// For a user-only scheduler DB. All the repack commands (repack add, rm, err and ls) are rejected. The tape state
  /// changes (tape ch --state) follow the rules of a user-only scheduler DB (Scheduler::OperatingMode::USER).
  NO_REPACK,
};

const std::string DISABLED_ADMIN_CMD_MSG {"Admin command disabled on this frontend."};

/// @brief Whether a command is rejected by the mode of the admin API (repack)
///
/// @param mode      The mode of the admin API
/// @param adminCmd  The command received
inline bool isCommandDisabled(AdminCmdMode mode, const cta::admin::AdminCmd& adminCmd) {
  return mode == AdminCmdMode::NO_REPACK && adminCmd.cmd() == cta::admin::AdminCmd::CMD_REPACK;
}

}  // namespace cta::admin_api

/// @brief Parses the "mode" of the [admin] TOML table
template<>
struct cta::common::FromString<cta::admin_api::AdminCmdMode> {
  static std::optional<cta::admin_api::AdminCmdMode> tryFrom(std::string_view text) {
    using enum cta::admin_api::AdminCmdMode;
    if (text == "all") {
      return ALL;
    } else if (text == "repack") {
      return REPACK;
    } else if (text == "no_repack") {
      return NO_REPACK;
    }
    return std::nullopt;
  }
};
