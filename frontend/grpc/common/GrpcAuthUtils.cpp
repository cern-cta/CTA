/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "GrpcAuthUtils.hpp"

#include "common/auth/JwtValidation.hpp"
#include "common/log/LogLevel.hpp"

using ::grpc::Status;
using ::grpc::StatusCode;

namespace cta::frontend::grpc::common {

std::pair<Status, std::string>
validateKrb5Token(const std::string& token, server::TokenStorage& tokenStorage, cta::log::LogContext& lc) {
  // Validate the Kerberos token from storage
  if (auto validationResult = tokenStorage.validate(token);
      validationResult != server::Krb5TokenValidationResult::NOT_FOUND) {
    // extract the name, construct the clientIdentity and remove the token
    std::string username = tokenStorage.getClientPrincipal(token);
    tokenStorage.remove(token);
    if (username.empty()) {
      return {Status(StatusCode::UNAUTHENTICATED, "Failed to retrieve client principal"), ""};
    }
    if (validationResult == server::Krb5TokenValidationResult::VALID) {
      return {Status::OK, username};
    } else {
      return {Status(StatusCode::UNAUTHENTICATED,
                     std::string("KRB5 authorization process error, expired token for client principal ") + username),
              ""};
    }
  }
  // else UNAUTHENTICATED
  return {Status(StatusCode::UNAUTHENTICATED, "KRB5 authorization process error. Invalid principal."), ""};
}

std::pair<Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
extractAuthHeaderAndValidate(const std::multimap<::grpc::string_ref, ::grpc::string_ref>& client_metadata,
                             bool jwtAuthEnabled,
                             std::optional<std::shared_ptr<cta::auth::JwkCache>> pubkeyCache,
                             server::TokenStorage& tokenStorage,
                             const std::string& ourHost,
                             const std::string& clientHost,
                             cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer sp(lc);

  std::string token;

  // Search for the authorization token in the metadata
  if (auto it = client_metadata.find("authorization"); it != client_metadata.end()) {
    // convert from grpc structure to string
    const ::grpc::string_ref& r = it->second;
    auto auth_header = std::string(r.data(), r.size());  // "Bearer <token>" or "Negotiate <token>"
    if (auth_header.starts_with("Bearer")) {
      // JWT Auth
      if (!jwtAuthEnabled) {
        return {Status(StatusCode::UNAUTHENTICATED, "Token authentication disabled on the CTA Frontend"), std::nullopt};
      }
      // Extract the token part, use substr(7) because that is the length of "Bearer" plus a space character
      token = auth_header.substr(7);
      if (token.empty()) {
        lc.log(cta::log::WARNING, "Authorization token missing");
        return {Status(StatusCode::UNAUTHENTICATED, "Missing Authorization token"), std::nullopt};
      }

      auto validationResult = cta::auth::ValidateJwt(token, pubkeyCache.value(), lc);

      if (validationResult.isValid) {
        lc.log(cta::log::DEBUG,
               "JWT token validation successful. Our host: '" + ourHost + "', client host: '" + clientHost + "'");
        cta::common::dataStructures::SecurityIdentity clientIdentity(validationResult.subjectClaim.value(),
                                                                     std::string(ourHost),
                                                                     std::string(clientHost),
                                                                     "grpc_token");
        return {Status::OK, clientIdentity};
      } else {
        lc.log(cta::log::WARNING, "JWT authorization process error. Token validation failed.");
        return {Status(StatusCode::UNAUTHENTICATED, "JWT authorization process error. Token validation failed."),
                std::nullopt};
      }
    } else if (auth_header.starts_with("Negotiate")) {
      // KRB5 auth
      // Extract the token part, use substr(10) because that is the length of "Negotiate" plus a space character
      token = auth_header.substr(10);
      if (token.empty()) {
        lc.log(cta::log::WARNING, "Authorization token missing");
        return {Status(StatusCode::UNAUTHENTICATED, "Missing Authorization token"), std::nullopt};
      }

      auto [validationStatus, username] = validateKrb5Token(token, tokenStorage, lc);

      if (validationStatus.ok()) {
        cta::common::dataStructures::SecurityIdentity clientIdentity(username,
                                                                     std::string(ourHost),
                                                                     std::string(clientHost),
                                                                     "grpc_krb5");
        return {Status::OK, clientIdentity};
      } else {
        lc.log(cta::log::WARNING, "Kerberos authorization process error. Token validation failed.");
        return {validationStatus, std::nullopt};
      }
    } else {
      // Unexpected authorization scheme
      lc.log(cta::log::ERR, "Unexpected authorization scheme (expected Bearer or Negotiate)");
      return {Status(StatusCode::UNAUTHENTICATED, "Unexpected authorization scheme"), std::nullopt};
    }
  } else {
    lc.log(cta::log::WARNING, "Authorization header missing");
    return {Status(StatusCode::UNAUTHENTICATED, "Missing Authorization header"), std::nullopt};
  }
}

}  // namespace cta::frontend::grpc::common
