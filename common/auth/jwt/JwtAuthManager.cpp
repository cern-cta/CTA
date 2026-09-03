/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JwtAuthManager.hpp"

#include "common/auth/jwt/RevokeList.hpp"
#include "common/exception/UserError.hpp"
#include "common/runtime/config/ConfigLoader.hpp"
#include "jwt-cpp/jwt.h"

namespace cta::auth {

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

    // check that the token has a gen claim and that it's at least the same as `min_generation`
    if (decoded.has_payload_claim("gen")) {
      if (auto genClaim = decoded.get_payload_claim("gen").as_integer();
          genClaim < static_cast<int64_t>(m_minGeneration)) {
        auto errorMessage = "Token generation is too old, minimum required is " + std::to_string(m_minGeneration);
        lc.log(cta::log::ERR, errorMessage);
        return {false, std::nullopt, errorMessage};
      }
    } else {
      auto errorMessage = "Token does not contain a 'gen' claim";
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

std::set<std::string, std::less<>> JwtAuthManager::loadRevokedJtis(const std::string& filePath) {
  const auto revokeFile = cta::runtime::loadFromToml<RevokeListFile>(filePath, false);

  // 'revoked_at' dates are interpreted as UTC
  std::set<std::string, std::less<>> revokedJtis;
  for (const auto& entry : revokeFile.revoked_tokens) {
    if (entry.jti.empty()) {
      throw cta::exception::UserError("revoked token entry in '" + filePath + "' has an empty JTI");
    } else if (entry.revoked_at == std::chrono::system_clock::time_point {}) {
      throw cta::exception::UserError("revoked token entry '" + entry.jti + "' in '" + filePath
                                      + "' has a missing or invalid revocation date");
    } else if (entry.revoked_at < std::chrono::system_clock::from_time_t(0)) {
      throw cta::exception::UserError("revoked token entry '" + entry.jti + "' in '" + filePath
                                      + "' has a revocation date before 1970");
    } else if (entry.reason.empty()) {
      throw cta::exception::UserError("revoked token entry '" + entry.jti + "' in '" + filePath
                                      + "' has a missing revocation reason");
    }

    revokedJtis.insert(entry.jti);
  }
  return revokedJtis;
}

}  // namespace cta::auth
