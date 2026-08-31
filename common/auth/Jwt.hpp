/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <time.h>

namespace cta::auth {
CTA_GENERATE_EXCEPTION_CLASS(CurlException);

struct TokenValidationResult {
  bool isValid;
  std::optional<std::string> subjectClaim;
  std::optional<std::string> errorMessage;
};

struct JwkCacheEntry {
  time_t last_refresh_time;
  std::string pubkey;  //!< public key in PEM format
};

class JwksFetcher {
public:
  virtual ~JwksFetcher() = default;
  virtual std::string fetchJWKS(const std::string& jwksUrl) = 0;
};

class CurlJwksFetcher : public JwksFetcher {
public:
  explicit CurlJwksFetcher(int totalTimeoutSecs);
  ~CurlJwksFetcher() override;

  // Delete copy/move to ensure single instance manages curl global state
  CurlJwksFetcher(const CurlJwksFetcher&) = delete;
  CurlJwksFetcher& operator=(const CurlJwksFetcher&) = delete;
  CurlJwksFetcher(CurlJwksFetcher&&) = delete;
  CurlJwksFetcher& operator=(CurlJwksFetcher&&) = delete;

  std::string fetchJWKS(const std::string& jwksUrl) override;

private:
  long m_totalTimeoutSecs;  //!< Total timeout in seconds
};

/**
 * A JSON Web (public) Key cache
 */
class JwkCache {
public:
  JwkCache(std::unique_ptr<JwksFetcher> jwksFetcher,
           const std::string& jwkUri,
           int pubKeyTTL,
           const cta::log::LogContext& lc)
      : m_jwksFetcher(std::move(jwksFetcher)),
        m_jwksUri(jwkUri),
        m_pubKeyTTL(pubKeyTTL),
        m_lc(lc) {}

  std::optional<JwkCacheEntry> find(const std::string& key);
  void update(time_t now);

  JwksFetcher& getFetcher() { return *m_jwksFetcher; }

private:
  std::unique_ptr<JwksFetcher> m_jwksFetcher;                  //!< used to fetch the JWKS file
  const std::string m_jwksUri;                                 //!< URL of JWKS file
  std::shared_mutex m_mutex;                                   //!< mutex to handle parallel requests
  std::map<std::string, JwkCacheEntry, std::less<>> m_keymap;  //!< the actual cache k -> v storage
  const int m_pubKeyTTL;                                       //!< TTL (s) of a public key entry
  cta::log::LogContext m_lc;                                   //!< The logging context
};

/**
 * This class encapsulates the verifications which have to be made
 * on JWTs, together with related caches/lists.
 */
class JwtAuthManager {
public:
  JwtAuthManager(std::unique_ptr<JwksFetcher> jwksFetcher,
                 const std::string& jwkUri,
                 int pubKeyTTL,
                 const std::string& expectedIssuer,
                 const std::string& expectedAudience,
                 const std::optional<std::string>& revokeListPath,
                 const cta::log::LogContext& lc)
      : m_pubKeyCache(JwkCache {std::move(jwksFetcher), jwkUri, pubKeyTTL, lc}),
        m_expectedIssuer(expectedIssuer),
        m_expectedAudience(expectedAudience),
        m_revokedSet(revokeListPath ? loadRevokedJtis(*revokeListPath) : std::set<std::string, std::less<>> {}) {}

  JwkCache& getCache() { return m_pubKeyCache; }

  void updateCache(time_t now) { m_pubKeyCache.update(now); }

  bool isRevoked(const std::string& jti) const { return m_revokedSet.contains(jti); }

  TokenValidationResult validateJwt(const std::string& encodedJwt, const log::LogContext& logContext);

private:
  /**
   * @brief Load the IDs of the revoked tokens from an external TOML file.
   *
   * The file must contain a top-level array of tables '[[revoked_tokens]]'. Every entry is
   * validated; a UserError is thrown for the first invalid one.
   *
   * @param filePath Path to the revoke-list TOML file.
   * @return The JTIs of all revoked tokens.
   */
  static std::set<std::string, std::less<>> loadRevokedJtis(const std::string& filePath);

  JwkCache m_pubKeyCache;                           //!< The public key cache
  std::string m_expectedIssuer;                     //!< The expected issuer for all tokens
  std::string m_expectedAudience;                   //!< The expected audience for all tokens
  std::set<std::string, std::less<>> m_revokedSet;  //!< A set of JWT IDs that have been revoked.
};
}  // namespace cta::auth
