/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "exception/Exception.hpp"
#include "log/LogContext.hpp"

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <time.h>

namespace cta {
CTA_GENERATE_EXCEPTION_CLASS(CurlException);

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
  CurlJwksFetcher(int totalTimeoutSecs = 30);
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

class JwkCache {
public:
  JwkCache(JwksFetcher& fetcher, const std::string& jwkUri, int pubkeyTimeout, const cta::log::LogContext& lc);

  void updateCache(time_t now);
  std::optional<JwkCacheEntry> find(const std::string& key);

private:
  JwksFetcher& m_jwksFetcher;
  std::string m_jwksUri;
  std::shared_mutex m_mutex;  //!< mutex to handle parallel requests
  std::map<std::string, JwkCacheEntry, std::less<>> m_keymap;
  //!< This gives the option to keep public keys around for longer than the refresh interval.
  int m_pubkeyTimeout;
  //!< The logging context
  cta::log::LogContext m_lc;  //!< always make a copy for thread safety
};
}  // namespace cta
