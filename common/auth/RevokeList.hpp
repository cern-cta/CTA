/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <toml++/toml.hpp>
#include <vector>

namespace cta::auth {

/**
 * @brief A revoked token entry
 */
struct RevokedTokenEntry final {
  std::string jti;             //!< The token's JTI (UUID)
  std::string reason;          //!< The token's revocation reason (only for audit purposes)
  toml::date_time revoked_at;  //!< The token's revocation date/time in UTC (only for audit purposes)

  static constexpr std::size_t memberCount() { return 3; }
};

/**
 * @brief Root schema of an external revoke-list TOML file
 */
struct RevokeListFile final {
  std::vector<RevokedTokenEntry> revoked_tokens;

  static constexpr std::size_t memberCount() { return 1; }
};

}  // namespace cta::auth
