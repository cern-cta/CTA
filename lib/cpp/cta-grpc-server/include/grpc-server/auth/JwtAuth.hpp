/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/auth/jwt/JwtAuthManager.hpp"
#include "common/log/Logger.hpp"
#include "grpc-server/config/AuthConfig.hpp"

#include <memory>
#include <thread>

namespace cta::grpc_server {

/**
 * @brief Owns the JWT authentication manager (JWKS cache + token checks) and the thread which periodically
 * refreshes the JWKS cache.
 *
 * When JWT is disabled in the configuration, manager() returns nullptr and no thread is started.
 * The refresh thread only holds a weak reference to the manager, so it exits on its own if the manager goes away.
 * Destroying this object stops and joins the refresh thread.
 */
class JwtAuth {
public:
  JwtAuth(const JwtAuthConfig& config, cta::log::Logger& logger);

  ~JwtAuth();

  // Non-copyable, non-movable: owns a thread which refers to this object's manager
  JwtAuth(const JwtAuth&) = delete;
  JwtAuth& operator=(const JwtAuth&) = delete;
  JwtAuth(JwtAuth&&) = delete;
  JwtAuth& operator=(JwtAuth&&) = delete;

  /**
   * @return The JWT auth manager, or nullptr if JWT authentication is disabled
   */
  std::shared_ptr<cta::auth::JwtAuthManager> manager() const { return m_manager; }

private:
  std::shared_ptr<cta::auth::JwtAuthManager> m_manager;
  std::jthread m_refreshThread;
};

}  // namespace cta::grpc_server
