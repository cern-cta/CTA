#pragma once

#include <string>
#include <map>
#include <time.h>
#include <iostream>
#include <functional>
#include <shared_mutex>
#include <optional>
#include <mutex>
#include <memory>

#include "exception/Exception.hpp"
#include "log/LogContext.hpp"

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
  std::string fetchJWKS(const std::string& jwksUrl) override;
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
