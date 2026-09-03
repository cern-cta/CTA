/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JwtAuthManager.hpp"

#include "common/auth/jwt/RevokeList.hpp"
#include "common/exception/UserError.hpp"
#include "common/runtime/config/ConfigLoader.hpp"
#include "common/utils/utils.hpp"
#include "jwt-cpp/jwt.h"

#include <chrono>

namespace cta::auth {

TokenValidationResult JwtAuthManager::validateJwt(const std::string& encodedJwt, log::Logger& logger) {
  /* The validation is done in the following order:

    1. Decode the token
    2. Get the 'kid' and fetch the corresponding public key
    3. Verify the token's signature, expiration and issuer
    4. Check whether the token has a 'jti' which is not on the revoke list
    5. Check that the token has a 'gen' claim and that it's at least the same as `min_generation`
    6. Check that the token has an 'aud' claim and that it matches the expected audience
    7. Check that there is a subject
    */

  auto lc = cta::log::LogContext(logger);
  auto params = cta::log::ScopedParamContainer(lc);
  try {
    auto decoded = jwt::decode(encodedJwt);

    // get the token header
    auto header = decoded.get_header_json();
    if (!header.contains("kid")) {
      auto errorMessage = "Token header does not contain a 'kid' field";
      params.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }
    std::string kid = header["kid"].get<std::string>();
    std::string pubkeyPem;

    params.add("jwt_kid", kid);

    // try to find our token's public key in our public key cache
    auto entry = m_pubKeyCache.find(kid);
    if (!entry.has_value()) {
      // cache miss -> let's fetch the information from the JWKS endpoint
      params.log(cta::log::INFO, "No cached key found, will fetch keys from endpoint");
      // add the key to the cache, after fetching
      const auto now = std::chrono::system_clock::now();
      time_t nowt = std::chrono::system_clock::to_time_t(now);
      m_pubKeyCache.update(nowt);
      entry = m_pubKeyCache.find(kid);

      if (!entry.has_value()) {
        // unable to fetch the public key for validation, fail the request
        auto errorMessage = "Unable to find the public key for the token, authentication failed";
        params.log(cta::log::ERR, errorMessage);
        return {false, std::nullopt, errorMessage};
      }
    }

    pubkeyPem = entry.value().pubkey;

    // validate the token's signature using the public key
    auto verifierChain = jwt::verify().allow_algorithm(jwt::algorithm::rs256(pubkeyPem, "", "", ""));

    params.add("jwt_iss", decoded.get_issuer());

    // validate the issuer of the JWT
    verifierChain.with_issuer(m_expectedIssuer);
    verifierChain.verify(decoded);

    if (!decoded.has_payload_claim("jti")) {
      auto errorMessage = "Token does not contain a 'jti' claim";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    // check whether the token has been revoked by any chance
    auto jti = decoded.get_payload_claim("jti").as_string();
    params.add("jwt_jti", jti);

    if (isRevoked(jti)) {
      lc.log(cta::log::ERR, "Token has been revoked");
      return {false, std::nullopt, "Token has been revoked"};
    }

    // check that the token has a gen claim and that it's at least the same as `min_generation`
    if (!decoded.has_payload_claim("gen")) {
      auto errorMessage = "Token does not contain a 'gen' claim";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    auto genClaim = decoded.get_payload_claim("gen").as_integer();
    params.add("jwt_gen", genClaim);

    if (genClaim < static_cast<int64_t>(m_minGeneration)) {
      auto errorMessage = "Token generation is too old, minimum required is " + std::to_string(m_minGeneration);
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    // The audience claim is mandatory and must match the expected audience.
    if (!decoded.has_payload_claim("aud")) {
      return {false, std::nullopt, "Token does not contain an 'aud' claim"};
    }

    auto audiences = decoded.get_audience();
    params.add("jwt_aud", cta::utils::joinCommaSeparated(audiences));

    // 'aud' is actually an array (there can be several audiences)
    if (!audiences.contains(m_expectedAudience)) {
      return {false, std::nullopt, "Token audience does not match expected value"};
    }

    if (!decoded.has_payload_claim("sub")) {
      auto errorMessage = "Token does not contain a 'sub' claim";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    // extract the "sub" claim
    auto subjectClaim = decoded.get_payload_claim("sub").as_string();
    params.add("jwt_sub", subjectClaim);

    // check that the token has an exp claim
    // technically, the expiration date is already verified by the verifierChain above,
    // but that will happily accept tokens with no expiration date
    if (!decoded.has_payload_claim("exp")) {
      auto errorMessage = "Token does not contain an 'exp' claim";
      lc.log(cta::log::ERR, errorMessage);
      return {false, std::nullopt, errorMessage};
    }

    params.add("jwt_exp", decoded.get_payload_claim("exp").as_integer());

    lc.log(cta::log::INFO, "Token validation successful");

    // everything fine, we've successfully validated the token
    return {true, subjectClaim, std::nullopt};
  } catch (const std::exception& e) {
    params.add(semconv::log::exceptionMessage, e.what());
    lc.log(cta::log::ERR, "Token validation failed due to an exception");
    return {false, std::nullopt, "Token validation failed: " + std::string(e.what())};
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
