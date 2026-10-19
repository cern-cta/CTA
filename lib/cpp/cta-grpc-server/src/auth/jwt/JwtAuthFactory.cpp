/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/auth/jwt/JwtAuthFactory.hpp"

#include "common/log/LogContext.hpp"
#include "common/log/LogLevel.hpp"
#include "grpc-server/auth/jwt/JwksFetcher.hpp"

#include <chrono>
#include <string>

namespace cta::grpc_server::auth {

std::shared_ptr<JwtAuthManager> makeJwtAuthManager(const std::optional<JwtAuthConfig>& config,
                                                   cta::log::Logger& logger) {
  if (!config.has_value() || !config->enabled) {
    return nullptr;
  }

  auto manager = std::make_shared<JwtAuthManager>(std::make_unique<CurlJwksFetcher>(config->jwks_total_timeout),
                                                  config->jwks_uri,
                                                  config->pub_key_timeout,
                                                  config->expected_issuer,
                                                  config->expected_audience,
                                                  config->min_generation,
                                                  config->revoke_list_path,
                                                  logger);
  manager->startCacheRefresh(std::chrono::seconds(config->cache_refresh_interval), logger);

  cta::log::LogContext lc(logger);
  cta::log::ScopedParamContainer spc(lc);
  spc.add("jwks_uri", config->jwks_uri)
    .add("total_timeout", std::to_string(config->jwks_total_timeout))
    .add("key_timeout", std::to_string(config->pub_key_timeout))
    .add("cache_refresh_interval", std::to_string(config->cache_refresh_interval));
  lc.log(cta::log::INFO, "JWT authentication enabled");
  return manager;
}

}  // namespace cta::grpc_server::auth
