/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "ValidateToken.hpp"
#include "jwt-cpp/jwt.h"

namespace cta {
TokenValidationResult
validateToken(const std::string& encodedJWT, std::shared_ptr<JwkCache> pubkeyCache, log::LogContext& logContext) {
  // this is thread-safe because it makes a copy of logContext for each thread
  cta::log::LogContext lc(logContext);
  cta::log::ScopedParamContainer sp(lc);
  try {
    auto decoded = jwt::decode(encodedJWT);
    // Extract the "sub" claim
    std::optional<std::string> subjectClaim = std::nullopt;
    if (decoded.has_payload_claim("sub")) {
      subjectClaim = decoded.get_payload_claim("sub").as_string();
      sp.add("extractedSubject", subjectClaim.value());
    } else {
      lc.log(cta::log::ERR, "Token does not contain a 'sub' claim");
      return {false, subjectClaim};
    }
    // Validation: check if the token is expired
    if (auto exp = decoded.get_payload_claim("exp").as_date(); exp < std::chrono::system_clock::now()) {
      lc.log(cta::log::WARNING, "In ValidateToken, Passed-in token has expired!");
      return {false, subjectClaim};  // Token has expired
    }
    auto header = decoded.get_header_json();
    std::string kid = header["kid"].get<std::string>();
    std::string pubkeyPem;

    // Get the JWKS endpoint, find the matching with our token, obtain the public key
    // used to sign the token and validate it
    // first try to use the cached value
    auto entry = pubkeyCache->find(kid);
    if (entry.has_value()) {
      pubkeyPem = entry.value().pubkey;
    } else {
      lc.log(cta::log::INFO, "No cached key found, will fetch keys from endpoint");
      sp.add("kid", kid);
      // add the key to the cache, after fetching
      const auto now = std::chrono::system_clock::now();
      time_t nowt = std::chrono::system_clock::to_time_t(now);
      pubkeyCache->updateCache(nowt);
      entry = pubkeyCache->find(kid);
      if (!entry.has_value()) {
        // unable to fetch the public key for validation, fail the request
        lc.log(cta::log::ERR, "Unable to find the public key for the token, authentication failed");
        return {false, subjectClaim};
      }
    }

    pubkeyPem = entry.value().pubkey;
    // Validate signature
    auto verifier = jwt::verify().allow_algorithm(jwt::algorithm::rs256(pubkeyPem, "", "", ""));
    verifier.verify(decoded);
    return {true, subjectClaim};
  } catch (const std::exception& e) {
    sp.add("exceptionMessage", e.what());
    lc.log(cta::log::ERR, "There was a failure in token verification");
    return {false, std::nullopt};
  }
}
}  // namespace cta
