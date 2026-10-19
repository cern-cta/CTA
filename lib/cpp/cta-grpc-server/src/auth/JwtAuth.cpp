/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/auth/JwtAuth.hpp"

#include "common/auth/jwt/JwksFetcher.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/LogLevel.hpp"
#include "common/semconv/Attributes.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stop_token>

namespace cta::grpc_server {

namespace {

/**
 * Periodically refresh the JWKS cache until stop is requested or the manager is destroyed.
 */
void jwksCacheRefreshLoop(std::stop_token stopToken,
                          std::weak_ptr<cta::auth::JwtAuthManager> weakAuthManager,
                          int cacheRefreshInterval,
                          cta::log::Logger& logger) {
  cta::log::LogContext threadLc(logger);
  threadLc.log(cta::log::INFO, "Detached JWKS cache refresh thread started");

  // Waits for the interval to elapse, or returns early if a stop is requested
  std::mutex mtx;
  std::condition_variable_any cv;
  std::unique_lock lock(mtx);
  while (!stopToken.stop_requested()) {
    cv.wait_for(lock, stopToken, std::chrono::seconds(cacheRefreshInterval), [] { return false; });
    if (stopToken.stop_requested()) {
      break;
    }
    auto authManager = weakAuthManager.lock();
    if (!authManager) {
      threadLc.log(cta::log::INFO, "jwtAuthManager no longer exists, exiting JWKS cache refresh thread");
      break;
    }
    time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    threadLc.log(cta::log::INFO, "Updating the JWKS cache");
    authManager->updateCache(now);
  }
  threadLc.log(cta::log::INFO, "Detached JWKS cache refresh thread ended");
}

}  // namespace

JwtAuth::JwtAuth(const JwtAuthConfig& config, cta::log::Logger& logger) {
  if (!config.enabled) {
    return;
  }

  auto jwksFetcher = std::make_unique<cta::auth::CurlJwksFetcher>(config.jwks_total_timeout);
  m_manager = std::make_shared<cta::auth::JwtAuthManager>(std::move(jwksFetcher),
                                                          config.jwks_uri,
                                                          config.pub_key_timeout,
                                                          config.expected_issuer,
                                                          config.expected_audience,
                                                          config.min_generation,
                                                          config.revoke_list_path,
                                                          logger);

  cta::log::LogContext lc(logger);
  {
    cta::log::ScopedParamContainer spc(lc);
    spc.add("jwks_uri", config.jwks_uri)
      .add("total_timeout", std::to_string(config.jwks_total_timeout))
      .add("key_timeout", std::to_string(config.pub_key_timeout))
      .add("cache_refresh_interval", std::to_string(config.cache_refresh_interval));
    lc.log(cta::log::INFO, "JWT authentication enabled");
  }

  lc.log(cta::log::INFO, "Starting the cache refresh thread for JWKS cache");
  // The logger must outlive this object (as it must for the JwtAuthManager)
  m_refreshThread = std::jthread(jwksCacheRefreshLoop,
                                 std::weak_ptr<cta::auth::JwtAuthManager>(m_manager),
                                 static_cast<int>(config.cache_refresh_interval),
                                 std::ref(logger));
}

JwtAuth::~JwtAuth() {
  if (m_refreshThread.joinable()) {
    m_refreshThread.request_stop();
    m_refreshThread.join();
  }
}

}  // namespace cta::grpc_server
