/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "GrpcAuthUtils.hpp"

#include "common/auth/JwtValidation.hpp"
#include "common/log/LogLevel.hpp"

namespace cta::frontend::grpc::common {

std::pair<::grpc::Status, std::string>
validateKrb5Token(const std::string& token, server::TokenStorage& tokenStorage, cta::log::LogContext& lc) {
  // Validate the Kerberos token from storage
  if (tokenStorage.validate(token)) {
    // extract the name, construct the clientIdentity and remove the token
    std::string username = tokenStorage.getClientPrincipal(token);
    tokenStorage.remove(token);
    if (username.empty()) {
      return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Failed to retrieve client principal"), ""};
    }
    return {::grpc::Status::OK, username};
  }
  // else UNAUTHENTICATED
  return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "KRB5 authorization process error. Invalid principal."),
          ""};
}

std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
extractAuthHeaderAndValidate(const std::multimap<::grpc::string_ref, ::grpc::string_ref>& client_metadata,
                             bool jwtAuthEnabled,
                             std::shared_ptr<cta::auth::JwkCache> pubkeyCache,
                             server::TokenStorage& tokenStorage,
                             const std::string& instanceName,
                             const std::string& peer,
                             cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer sp(lc);

  std::string token;

  // Search for the authorization token in the metadata
  if (auto it = client_metadata.find("authorization"); it != client_metadata.end()) {
    // convert from grpc structure to string
    const ::grpc::string_ref& r = it->second;
    auto auth_header = std::string(r.data(), r.size());  // "Bearer <token>" or "Negotiate <token>"
    if (auth_header.substr(0, 6) == "Bearer") {
      // JWT Auth
      if (!jwtAuthEnabled) {
        return {
          ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Token authentication disabled on the CTA Frontend"),
          std::nullopt};
      }
      token = auth_header.substr(
        7);  // Extract the token part, use substr(7) because that is the length of "Bearer" plus a space character
      if (token.empty()) {
        lc.log(cta::log::WARNING, "Authorization token missing");
        return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Missing Authorization token"), std::nullopt};
      }

      auto validationResult = cta::auth::ValidateJwt(token, pubkeyCache, lc);

      if (validationResult.isValid) {
        cta::common::dataStructures::SecurityIdentity clientIdentity(validationResult.subjectClaim.value(), peer);
        return {::grpc::Status::OK, clientIdentity};
      } else {
        lc.log(cta::log::WARNING, "JWT authorization process error. Token validation failed.");
        return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED,
                               "JWT authorization process error. Token validation failed."),
                std::nullopt};
      }
    } else if (auth_header.substr(0, 9) == "Negotiate") {
      // KRB5 auth
      token = auth_header.substr(
        10);  // Extract the token part, use substr(10) because that is the length of "Negotiate" plus a space character
      if (token.empty()) {
        lc.log(cta::log::WARNING, "Authorization token missing");
        return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Missing Authorization token"), std::nullopt};
      }

      auto [validationStatus, username] = validateKrb5Token(token, tokenStorage, lc);

      if (validationStatus.ok()) {
        cta::common::dataStructures::SecurityIdentity clientIdentity(username, peer);
        return {::grpc::Status::OK, clientIdentity};
      } else {
        lc.log(cta::log::WARNING, "Kerberos authorization process error. Token validation failed.");
        return {validationStatus, std::nullopt};
      }
    } else {
      // Unexpected authorization scheme
      lc.log(cta::log::ERR, "Unexpected authorization scheme (expected Bearer or Negotiate)");
      return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Unexpected authorization scheme"), std::nullopt};
    }
  } else {
    lc.log(cta::log::WARNING, "Authorization header missing");
    return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Missing Authorization header"), std::nullopt};
  }
}

}  // namespace cta::frontend::grpc::common
