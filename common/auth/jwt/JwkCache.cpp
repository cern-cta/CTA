/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JwkCache.hpp"

#include "jwt-cpp/jwt.h"

#include <curl/curl.h>

namespace cta::auth {

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

void JwkCache::update(time_t now) {
  log::LogContext lc(m_lc);
  log::ScopedParamContainer spc(lc);
  lc.log(log::DEBUG, "In function update()");
  std::string raw_jwks;
  try {
    raw_jwks = m_jwksFetcher->fetchJWKS(m_jwksUri);
  } catch (CurlException& ex) {
    lc.log(log::ERR, ex.getMessageValue());
    return;
  }
  // purge any keys that have expired
  lc.log(log::DEBUG, "In function update(), waiting to acquire unique lock");
  std::unique_lock lock(m_mutex);
  lc.log(log::DEBUG, "In function update(), just acquired the unique lock");

  std::erase_if(m_keymap, [now, &lc, this](auto item) {
    bool doErase = (m_pubKeyTTL != 0) && (item.second.last_refresh_time + m_pubKeyTTL <= now);
    if (doErase) {
      lc.log(log::DEBUG, std::string("Removing entry for key with kid ") + item.first);
    }
    return doErase;
  });

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
        lc.log(log::WARNING, "Field \"x5c\" missing from JWKS entry '" + kid + "', skipping it");
        continue;
      }
      if (kid.empty()) {
        lc.log(log::WARNING, "Field \"kid\" missing from JWKS entry, skipping it");
        continue;
      }
    } catch (std::runtime_error& ex) {
      spc.add(semconv::log::exceptionMessage, ex.what());
      lc.log(log::WARNING, "Runtime error thrown when parsing JWKS entry '" + kid + "', skipping it");
      continue;
    }

    std::string pubkeyPem = jwt::helper::convert_base64_der_to_pem(x5c);
    JwkCacheEntry entry = {now, pubkeyPem};
    m_keymap[kid] = entry;
    lc.log(log::INFO, "Adding new key entry in cache");
    spc.add("kid", kid);
    spc.add("cachedTime", std::to_string(now));
  }
}
}  // namespace cta::auth
