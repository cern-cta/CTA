/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"
#include "grpc-server/auth/jwt/JwtAuthManager.hpp"
#include "grpc-server/config/AuthConfig.hpp"

#include <memory>
#include <optional>

namespace cta::grpc_server::auth {

/**
 * @brief Build the JWT auth manager and start its periodic JWKS cache refresh.
 *
 * @param config The JWT configuration, if any
 * @param logger The logger used by the manager and its refresh thread; must outlive the returned manager
 * @return The JWT auth manager, or nullptr if JWT authentication is not configured or disabled
 */
std::shared_ptr<JwtAuthManager> makeJwtAuthManager(const std::optional<JwtAuthConfig>& config,
                                                   cta::log::Logger& logger);

}  // namespace cta::grpc_server::auth
