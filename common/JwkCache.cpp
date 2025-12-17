/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JwkCache.hpp"

#include "jwt-cpp/jwt.h"

#include <curl/curl.h>
#include <mutex>

namespace cta {
JwkCache::JwkCache(JwksFetcher& fetcher, const std::string& jwkUri, int pubkeyTimeout, const log::LogContext& lc)
    : m_jwksFetcher(fetcher),
      m_jwksUri(jwkUri),
      m_pubkeyTimeout(pubkeyTimeout),
      m_lc(lc) {};

// Function to handle curl responses
size_t WriteCallback(char* contents, size_t size, size_t nmemb, std::string* output) {
  size_t totalSize = size * nmemb;
  output->append(contents, totalSize);
  return totalSize;
};

std::string CurlJwksFetcher::fetchJWKS(const std::string& jwksUrl) {
  CURL* curl;
  CURLcode res;
  std::string readBuffer;

  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, jwksUrl.c_str());
    // use TLS 1.2 or later
    curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      throw CurlException(std::string("CURL failed in fetchJWKS: ") + curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl);
  } else {
    throw CurlException("CURL failed to call curl_easy_init()");
  }
  curl_global_cleanup();

  return readBuffer;
}

std::optional<JwkCacheEntry> JwkCache::find(const std::string& key) {
  log::LogContext lc(m_lc);
  lc.log(log::DEBUG, "Waiting to acquire shared_lock in JwkCache::find");
  std::shared_lock lock(m_mutex);
  lc.log(log::DEBUG, "Just acquired the shared_lock in JwkCache::find");
  auto it = m_keymap.find(key);
  if (it == m_keymap.end()) {
    lc.log(log::INFO, std::string("Entry not found for kid ") + key);
    return std::nullopt;
  } else {
    lc.log(log::INFO, std::string("Entry found in cache for kid ") + key);
    return std::optional<JwkCacheEntry>(it->second);
  }
}

void JwkCache::updateCache(time_t now) {
  log::LogContext lc(m_lc);
  log::ScopedParamContainer spc(lc);
  lc.log(log::DEBUG, "In function updateCache");
  std::string raw_jwks;
  try {
    raw_jwks = m_jwksFetcher.fetchJWKS(m_jwksUri);
  } catch (CurlException& ex) {
    lc.log(log::ERR, ex.getMessageValue());
    return;
  }
  // purge any keys that have expired
  lc.log(log::DEBUG, "In function updateCache, waiting to acquire unique lock");
  std::unique_lock<std::shared_mutex> lock(m_mutex);
  lc.log(log::DEBUG, "In updateCache, just acquired the unique lock");
  for (auto it = m_keymap.begin(); it != m_keymap.end();) {
    auto lastRefresh = it->second.last_refresh_time;
    // if pubkeyTimeout is 0, then we don't want public keys to expire
    if ((m_pubkeyTimeout != 0) && (lastRefresh + m_pubkeyTimeout <= now)) {
      lc.log(log::DEBUG, std::string("Removing entry for key with kid ") + it->first);
      it = m_keymap.erase(it);  // erase returns next valid iterator
    } else {
      ++it;
    }
  }
  // add the new keys
  auto jwks = jwt::parse_jwks(raw_jwks);
  std::string kid;
  std::string x5c;
  // now iterate over the keys, add the key if it's used for signing
  for (const auto& jwk : jwks) {
    try {
      if (std::string use = jwk.get_use(); use != "sig") {
        continue;
      }
      kid = jwk.get_key_id();
      x5c = jwk.get_x5c_key_value();
      if (x5c.empty()) {
        lc.log(log::ERR, "Field \"x5c\" missing from JWKS entry");
        return;
      }
      if (kid.empty()) {
        lc.log(log::ERR, "Field \"kid\" missing from JWKS entry");
        return;
      }
    } catch (std::runtime_error& ex) {
      spc.add("exceptionMessage", ex.what());
      lc.log(log::ERR, "Runtime error thrown when parsing JWKS");
      return;
    }

    std::string pubkeyPem = jwt::helper::convert_base64_der_to_pem(x5c);
    JwkCacheEntry entry = {now, pubkeyPem};
    m_keymap[kid] = entry;
    lc.log(log::INFO, "Adding new key entry in cache");
    spc.add("kid", kid);
    spc.add("cachedTime", std::to_string(now));
  }
}
}  // namespace cta
