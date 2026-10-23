/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "grpc-server/auth/jwt/JwkCache.hpp"
#include "grpc-server/auth/jwt/JwksFetcher.hpp"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>

namespace cta::grpc_server::auth {
/// This class encapsulates the verifications which have to be made
/// on JWTs, together with related caches/lists.
class JwtAuthManager {
public:
  JwtAuthManager(std::unique_ptr<JwksFetcher> jwksFetcher,
                 const std::string& jwkUri,
                 int pubKeyTTL,
                 const std::string& expectedIssuer,
                 const std::string& expectedAudience,
                 uint32_t minGeneration,
                 const std::optional<std::string>& revokeListPath,
                 log::Logger& logger)
      : m_pubKeyCache(JwkCache {std::move(jwksFetcher), jwkUri, pubKeyTTL, logger}),
        m_expectedIssuer(expectedIssuer),
        m_expectedAudience(expectedAudience),
        m_minGeneration(minGeneration),
        m_revokedSet(revokeListPath ? loadRevokedJtis(*revokeListPath) : std::set<std::string, std::less<>> {}) {}

  JwkCache& getCache() { return m_pubKeyCache; }

  void updateCache(time_t now) { m_pubKeyCache.update(now); }

  /// @brief Start a background thread which refreshes the JWKS cache every @p interval.
  ///
  /// The thread is stopped and joined when this object is destroyed. Must be called at most once.
  /// @param interval The time between two cache refreshes
  /// @param logger The logger used by the refresh thread; must outlive this object
  void startCacheRefresh(std::chrono::seconds interval, log::Logger& logger);

  bool isRevoked(const std::string& jti) const { return m_revokedSet.contains(jti); }

  TokenValidationResult validateJwt(const std::string& encodedJwt, log::Logger& logger);

private:
  /// @brief Load the IDs of the revoked tokens from an external TOML file.
  ///
  /// The file must contain a top-level array of tables '[[revoked_tokens]]'. Every entry is
  /// validated; a UserError is thrown for the first invalid one.
  ///
  /// @param filePath Path to the revoke-list TOML file.
  /// @return The JTIs of all revoked tokens.
  static std::set<std::string, std::less<>> loadRevokedJtis(const std::string& filePath);

  /// The public key cache
  JwkCache m_pubKeyCache;
  /// The expected issuer for all tokens
  std::string m_expectedIssuer;
  /// The expected audience for all tokens
  std::string m_expectedAudience;
  /// The minimum token generation (version) required
  uint32_t m_minGeneration;
  /// A set of JWT IDs that have been revoked.
  std::set<std::string, std::less<>> m_revokedSet;

  /// Declared last so that it is joined before the other members are destroyed
  ///
  /// Periodic JWKS cache refresh, see startCacheRefresh()
  std::optional<std::jthread> m_cacheRefreshThread;
};
}  // namespace cta::grpc_server::auth
