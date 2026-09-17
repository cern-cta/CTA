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

  // parse the key data
  auto jwks = jwt::parse_jwks(raw_jwks);

  // now iterate over the keys
  for (const auto& jwk : jwks) {
    std::string kid;
    try {
      if (std::string use = jwk.get_use(); use != "sig") {
        continue;
      } else if (!jwk.has_key_id()) {
        lc.log(log::WARNING, "Field \"kid\" missing from JWKS entry, skipping it");
        continue;
      } else if (!jwk.has_key_type()) {
        lc.log(log::WARNING, "Field \"kty\" missing from JWKS entry, skipping it");
        continue;
      } else if (auto keyType = jwk.get_key_type(); keyType != "RSA") {
        lc.log(log::WARNING, "JWKS entry has type '" + keyType + "', which is not supported");
        continue;
      }
      kid = jwk.get_key_id();
    } catch (std::runtime_error& ex) {
      spc.add(semconv::log::exceptionMessage, ex.what());
      lc.log(log::WARNING, "Runtime error thrown when parsing JWKS entry '" + kid + "', skipping it");
      continue;
    }

    // we should construct the PEM from the modulus and exponent
    if (!jwk.has_jwk_claim("n") || !jwk.has_jwk_claim("e")) {
      lc.log(log::WARNING, "JWKS entry doesn't have both 'n' and 'e' claims, skipping it");
      continue;
    }

    // build PEM from n and e (modulus and exponent)
    auto n = jwk.get_jwk_claim("n").as_string();
    auto e = jwk.get_jwk_claim("e").as_string();

    std::error_code ec;
    std::string pubkeyPem = jwt::helper::create_public_key_from_rsa_components(n, e, ec);

    // something went wrong
    if (ec) {
      lc.log(log::WARNING, "Couldn't build PEM from modulus and exponent, skipping JWKS entry");
      continue;
    }

    JwkCacheEntry entry = {now, pubkeyPem};
    m_keymap[kid] = entry;
    lc.log(log::INFO, "Adding new key entry in cache");
    spc.add("kid", kid);
    spc.add("cachedTime", std::to_string(now));
  }
}
}  // namespace cta::auth
