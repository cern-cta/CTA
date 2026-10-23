/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/auth/Authentication.hpp"

#include "common/log/LogLevel.hpp"
#include "common/utils/utils.hpp"
#include "grpc-server/auth/jwt/JwtAuthManager.hpp"

#include <algorithm>
#include <iterator>
#include <variant>
#include <vector>

using ::grpc::Status;
using ::grpc::StatusCode;

using cta::common::dataStructures::SecurityIdentity;

namespace cta::grpc_server::auth {

namespace {

using AuthResult = std::pair<Status, std::optional<SecurityIdentity>>;

/// @brief Finds the "authorization" header, or logs and returns the error to send back to the client
std::variant<std::string, AuthResult> findAuthHeader(const GrpcMetadata& clientMetadata, cta::log::LogContext& lc) {
  if (auto it = clientMetadata.find("authorization"); it != clientMetadata.end()) {
    // convert from grpc structure to string: "Bearer <token>"
    return std::string(it->second.data(), it->second.size());
  }
  lc.log(cta::log::WARNING, "Authorization header missing");
  return AuthResult {Status(StatusCode::UNAUTHENTICATED, "Missing Authorization header"), std::nullopt};
}

/// @brief Extracts the token following the scheme, which is "prefixLength" characters including the space
std::string extractToken(const std::string& authHeader, size_t prefixLength) {
  return authHeader.size() > prefixLength ? authHeader.substr(prefixLength) : std::string();
}

/// @brief Validates the JWT of an "authorization" header known to use the Bearer scheme
///
/// @return OK with a JWT SecurityIdentity, or UNAUTHENTICATED if the token is missing or fails validation
AuthResult validateBearerHeader(const std::string& authHeader,
                                JwtAuthManager& jwtAuthManager,
                                const std::string& ourInstance,
                                const std::string& clientHost,
                                cta::log::LogContext& lc) {
  // use 7 because that is the length of "Bearer" plus a space character
  const std::string token = extractToken(authHeader, 7);
  if (token.empty()) {
    lc.log(cta::log::WARNING, "Authorization token missing");
    return {Status(StatusCode::UNAUTHENTICATED, "Missing Authorization token"), std::nullopt};
  }

  auto validationResult = jwtAuthManager.validateJwt(token, lc.logger());

  if (validationResult.isValid) {
    lc.log(cta::log::DEBUG, "JWT token validation successful. Client host: '" + clientHost + "'");
    SecurityIdentity clientIdentity(validationResult.subjectClaim.value(),
                                    std::string(ourInstance),
                                    std::string(clientHost),
                                    SecurityIdentity::Protocol::JWT);
    return {Status::OK, clientIdentity};
  }

  const auto& errorMsg = validationResult.errorMessage.value_or("Unknown token validation error");
  {
    cta::log::ScopedParamContainer params(lc);
    params.add(cta::semconv::log::errorMessage, errorMsg);
    lc.log(cta::log::WARNING, "JWT token validation failed");
  }
  return {Status(StatusCode::UNAUTHENTICATED, errorMsg), std::nullopt};
}

/// @brief Logs the unsupported scheme and builds the UNAUTHENTICATED result; "expected" names the accepted scheme
AuthResult unexpectedScheme(const char* expected, cta::log::LogContext& lc) {
  lc.log(cta::log::ERR, std::string("Unexpected authorization scheme (expected ") + expected + ")");
  return {Status(StatusCode::UNAUTHENTICATED, "Unexpected authorization scheme"), std::nullopt};
}

}  // namespace

std::pair<Status, std::optional<SecurityIdentity>>
authenticateMtls(::grpc::ServerContext* context,
                 const std::string& clientHost,
                 const std::string& serverInstanceName,
                 const std::set<std::string, std::less<>>& acceptedIdentities,
                 cta::log::LogContext& lc) {
  // fetch the certificate identities from the auth context
  auto certIdents = context->auth_context()->GetPeerIdentity();

  std::set<std::string, std::less<>> certIdentities;
  std::ranges::transform(certIdents, std::inserter(certIdentities, certIdentities.begin()), [](const auto& ref) {
    return std::string(ref.data(), ref.size());
  });

  lc.log(cta::log::DEBUG,
         "Received mTLS identities: "
           + cta::utils::joinCommaSeparated(std::vector(certIdentities.begin(), certIdentities.end())));

  lc.log(cta::log::DEBUG,
         "Resolved certificate identities for instance: "
           + cta::utils::joinCommaSeparated(std::vector(acceptedIdentities.begin(), acceptedIdentities.end())));

  std::set<std::string, std::less<>> intersection;
  std::ranges::set_intersection(certIdentities, acceptedIdentities, std::inserter(intersection, intersection.begin()));

  if (intersection.empty()) {
    lc.log(cta::log::ERR, "Name " + clientHost + " doesn't match any of the certificate identities.");
    return {Status(StatusCode::UNAUTHENTICATED, "Certificate doesn't match identity"), std::nullopt};
  }

  SecurityIdentity clientIdentity(clientHost, serverInstanceName, context->peer(), SecurityIdentity::Protocol::MTLS);
  return {Status::OK, clientIdentity};
}

std::pair<Status, std::optional<SecurityIdentity>> authenticateJwt(const GrpcMetadata& client_metadata,
                                                                   JwtAuthManager& jwtAuthManager,
                                                                   const std::string& ourInstance,
                                                                   const std::string& clientHost,
                                                                   cta::log::LogContext& lc) {
  auto header = findAuthHeader(client_metadata, lc);
  if (auto* error = std::get_if<AuthResult>(&header)) {
    return std::move(*error);
  }
  const auto& authHeader = std::get<std::string>(header);
  if (!authHeader.starts_with("Bearer")) {
    return unexpectedScheme("Bearer", lc);
  }
  return validateBearerHeader(authHeader, jwtAuthManager, ourInstance, clientHost, lc);
}

}  // namespace cta::grpc_server::auth
