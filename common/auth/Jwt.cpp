/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Jwt.hpp"

#include "jwt-cpp/jwt.h"

#include <curl/curl.h>
#include <mutex>

namespace cta::auth {

// Function to handle curl responses
size_t WriteCallback(char* contents, size_t size, size_t nmemb, std::string* output) {
  size_t totalSize = size * nmemb;
  output->append(contents, totalSize);
  return totalSize;
};

CurlJwksFetcher::CurlJwksFetcher(int totalTimeoutSecs) : m_totalTimeoutSecs(totalTimeoutSecs) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
}

CurlJwksFetcher::~CurlJwksFetcher() {
  curl_global_cleanup();
}

std::string CurlJwksFetcher::fetchJWKS(const std::string& jwksUrl) {
  CURL* curl;
  CURLcode res;
  std::string readBuffer;

  curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, jwksUrl.c_str());
    // use TLS 1.2 or later
    curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);
    // Set timeouts to prevent indefinite hangs
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, m_totalTimeoutSecs);
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

TokenValidationResult JwtAuthManager::validateJwt(const std::string& encodedJwt, const log::LogContext& logContext) {
  /* The validation is done in the following order:

    1. Decode the token
    2. Get the 'kid' and fetch the corresponding public key
    3. Verify the token's signature, expiration, and (optionally) issuer
    4. Check whether the token's 'jti' is in the revoke list
    5. Check that there is a subject
  */

  // this is thread-safe because it makes a copy of logContext for each thread
  cta::log::LogContext lc(logContext);
  cta::log::ScopedParamContainer sp(lc);
  try {
    auto decoded = jwt::decode(encodedJwt);

    // get the token header
    auto header = decoded.get_header_json();
    if (!header.contains("kid")) {
      auto errorMessage = "Token header does not contain a 'kid' field";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }
    std::string kid = header["kid"].get<std::string>();
    std::string pubkeyPem;

    // try to find our token's public key in our public key cache
    auto entry = m_pubKeyCache.find(kid);
    if (!entry.has_value()) {
      // cache miss -> let's fetch the information from the JWKS endpoint
      lc.log(cta::log::INFO, "No cached key found, will fetch keys from endpoint");
      sp.add("kid", kid);
      // add the key to the cache, after fetching
      const auto now = std::chrono::system_clock::now();
      time_t nowt = std::chrono::system_clock::to_time_t(now);
      m_pubKeyCache.update(nowt);
      entry = m_pubKeyCache.find(kid);
      if (!entry.has_value()) {
        // unable to fetch the public key for validation, fail the request
        auto errorMessage = "Unable to find the public key for the token, authentication failed";
        lc.log(cta::log::ERR, errorMessage);
        return {false, std::nullopt, errorMessage};
      }
    }

    pubkeyPem = entry.value().pubkey;

    // validate the token's signature using the public key
    auto verifierChain = jwt::verify().allow_algorithm(jwt::algorithm::rs256(pubkeyPem, "", "", ""));

    // validate the issuer of the JWT
    verifierChain.with_issuer(m_expectedIssuer);
    verifierChain.verify(decoded);

    // The audience claim is mandatory and must match the expected audience.
    if (!decoded.has_payload_claim("aud")) {
      return {false, std::nullopt, "Token does not contain an 'aud' claim"};
    }
    // 'aud' is actually an array (there can be several audiences)
    if (auto audiences = decoded.get_audience(); !audiences.contains(m_expectedAudience)) {
      return {false, std::nullopt, "Token audience does not match expected value"};
    }

    // check whether the token has been revoked by any chance
    if (decoded.has_payload_claim("jti")) {
      auto jti = decoded.get_payload_claim("jti").as_string();
      lc.log(cta::log::DEBUG, "Token JTI: " + jti);
      if (isRevoked(jti)) {
        auto errorMessage = "Token '" + jti + "' has been revoked";
        lc.log(cta::log::ERR, errorMessage);
        return {false, std::nullopt, errorMessage};
      }
    } else {
      auto errorMessage = "Token does not contain a 'jti' claim";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    // extract the "sub" claim
    std::optional<std::string> subjectClaim = std::nullopt;
    if (decoded.has_payload_claim("sub")) {
      subjectClaim = decoded.get_payload_claim("sub").as_string();
      sp.add("extractedSubject", subjectClaim.value());
    } else {
      auto errorMessage = "Token does not contain a 'sub' claim";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    // check that the token has an exp claim
    // technically, the expiration date is already verified by the verifierChain above,
    // but that will happily accept tokens with no expiration date
    if (!decoded.has_payload_claim("exp")) {
      auto errorMessage = "Token does not contain an 'exp' claim";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    // everything fine, we've successfully validated the token
    return {true, subjectClaim, std::nullopt};
  } catch (const std::exception& e) {
    sp.add(semconv::log::exceptionMessage, e.what());
    auto errorMessage = "Token validation failed";
    lc.log(cta::log::ERR, errorMessage);
    return {false, std::nullopt, errorMessage};
  }
}

}  // namespace cta::auth
