#include "ValidateToken.hpp"
#include "jwt-cpp/jwt.h"

namespace cta {
bool ValidateToken(const std::string& encodedJWT, JwkCache& pubkeyCache, log::LogContext logContext) {
  // this is thread-safe because it makes a copy of logContext for each thread
  cta::log::LogContext lc(logContext);
  cta::log::ScopedParamContainer sp(lc);
  try {
    auto decoded = jwt::decode(encodedJWT);
    // Example validation: check if the token is expired
    auto exp = decoded.get_payload_claim("exp").as_date();
    if (exp < std::chrono::system_clock::now()) {
        lc.log(cta::log::WARNING, "In ValidateToken, Passed-in token has expired!");
        return false;  // Token has expired
    }
    auto header = decoded.get_header_json();
    std::string kid = header["kid"].get<std::string>();
    std::string alg = header["alg"].get<std::string>(); // alg should be RS256
    std::string pubkeyPem;

    // Get the JWKS endpoint, find the matching with our token, obtain the public key
    // used to sign the token and validate it
    // first try to use the cached value
    auto entry = pubkeyCache.find(kid);
    if (!entry.has_value()) {
        lc.log(cta::log::INFO, "No cached key found, will fetch keys from endpoint");
        sp.add("kid", kid);
    } else {
      pubkeyPem = entry.value().pubkey;
    }
    if (!entry.has_value()) {
      // add the key to the cache, after fetching
      auto const now = std::chrono::system_clock::now();
      time_t nowt = std::chrono::system_clock::to_time_t(now);
      pubkeyCache.updateCache(nowt);
      entry = pubkeyCache.find(kid);
      if (!entry.has_value()) {
        // unable to fetch the public key for validation, fail the request
        lc.log(cta::log::WARNING, "Unable to find the public key for the token, authentication failed");
        return false;
      }
    }

    pubkeyPem = entry.value().pubkey;
    // Validate signature
    auto verifier = jwt::verify()
                        .allow_algorithm(jwt::algorithm::rs256(pubkeyPem, "", "", ""));
                        // .allow_algorithm(jwt::algorithm::rs256(x5c));
                        // .with_issuer("http://auth-keycloak:8080/realms/master");
    verifier.verify(decoded);
    return true;
  } catch (const std::system_error& e) {
    lc.log(cta::log::ERR, std::string("There was a failure in token verification. ") + e.what());
    return false;
  } catch (const std::runtime_error& e) {
    lc.log(cta::log::ERR, std::string("Failure in token verification, ") + e.what());
    return false;
  } catch (...) {
    lc.log(cta::log::ERR, "Unknown exception thrown in ValidateToken");
    return false;
  }
}
}