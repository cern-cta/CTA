/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/auth/jwt/JwksFetcher.hpp"
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

struct TokenValidationResult {
  bool isValid;
  std::optional<std::string> subjectClaim;
  std::optional<std::string> errorMessage;
};

struct JwkCacheEntry {
  time_t last_refresh_time;
  std::string pubkey;  //!< public key in PEM format
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

}  // namespace cta::auth
