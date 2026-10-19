/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rpc/auth/RequestAuthenticator.hpp"

#include "common/log/LogLevel.hpp"

#include <variant>

using ::grpc::Status;
using ::grpc::StatusCode;

using cta::common::dataStructures::SecurityIdentity;

namespace cta::admin_api::rpc {

namespace {

using AuthResult = std::pair<Status, std::optional<SecurityIdentity>>;

/**
 * @brief Validates a Kerberos token against the token store. The token is removed from the store once looked up.
 *
 * @return Pair of gRPC Status and the client principal (empty unless validation succeeds)
 */
std::pair<Status, std::string>
validateKrb5Token(const std::string& token, kerberos::TokenStore& krbTokenStore, cta::log::LogContext& lc) {
  // Validate the Kerberos token from storage
  if (auto validationResult = krbTokenStore.validate(token);
      validationResult != kerberos::Krb5TokenValidationResult::NOT_FOUND) {
    // extract the name, construct the clientIdentity and remove the token
    std::string username = krbTokenStore.getClientPrincipal(token);
    krbTokenStore.remove(token);
    if (username.empty()) {
      return {Status(StatusCode::UNAUTHENTICATED, "Failed to retrieve client principal"), ""};
    }
    if (validationResult == kerberos::Krb5TokenValidationResult::VALID) {
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

/**
 * @brief Validates the Kerberos token of an "authorization" header known to use the Negotiate scheme
 *
 * @return OK with a KRB5 SecurityIdentity, or UNAUTHENTICATED if the token is missing or fails validation
 */
AuthResult validateNegotiateHeader(const std::string& authHeader,
                                   kerberos::TokenStore& krbTokenStore,
                                   const std::string& ourInstance,
                                   const std::string& clientHost,
                                   cta::log::LogContext& lc) {
  // use 10 because that is the length of "Negotiate" plus a space character
  const std::string token = authHeader.size() > 10 ? authHeader.substr(10) : std::string();
  if (token.empty()) {
    lc.log(cta::log::WARNING, "Authorization token missing");
    return {Status(StatusCode::UNAUTHENTICATED, "Missing Authorization token"), std::nullopt};
  }

  auto [validationStatus, username] = validateKrb5Token(token, krbTokenStore, lc);

  if (validationStatus.ok()) {
    SecurityIdentity clientIdentity(username, ourInstance, clientHost, SecurityIdentity::Protocol::KRB5);
    return {Status::OK, clientIdentity};
  }

  lc.log(cta::log::WARNING, "Kerberos authorization process error. Token validation failed.");
  return {validationStatus, std::nullopt};
}

}  // namespace

RequestAuthenticator::RequestAuthenticator(std::shared_ptr<cta::auth::JwtAuthManager> jwtAuthManager,
                                           std::optional<std::reference_wrapper<kerberos::TokenStore>> krbTokenStore,
                                           std::string ourInstance)
    : m_jwtAuthManager(std::move(jwtAuthManager)),
      m_krbTokenStore(krbTokenStore),
      m_ourInstance(std::move(ourInstance)) {}

std::pair<Status, std::optional<SecurityIdentity>>
RequestAuthenticator::authenticate(const grpc_server::GrpcMetadata& clientMetadata,
                                   const std::string& clientHost,
                                   cta::log::LogContext& lc) const {
  auto it = clientMetadata.find("authorization");
  if (it == clientMetadata.end()) {
    lc.log(cta::log::WARNING, "Authorization header missing");
    return {Status(StatusCode::UNAUTHENTICATED, "Missing Authorization header"), std::nullopt};
  }
  // "Bearer <token>" or "Negotiate <token>"
  const std::string authHeader(it->second.data(), it->second.size());

  if (authHeader.starts_with("Bearer")) {
    if (!m_jwtAuthManager) {
      return {Status(StatusCode::UNAUTHENTICATED, "Token authentication disabled on the CTA Frontend"), std::nullopt};
    }
    return grpc_server::authenticateJwt(clientMetadata, *m_jwtAuthManager, m_ourInstance, clientHost, lc);
  }

  if (authHeader.starts_with("Negotiate")) {
    if (!m_krbTokenStore) {
      return {Status(StatusCode::UNAUTHENTICATED, "Kerberos authentication disabled on the CTA Frontend"),
              std::nullopt};
    }
    return validateNegotiateHeader(authHeader, m_krbTokenStore->get(), m_ourInstance, clientHost, lc);
  }

  lc.log(cta::log::ERR, "Unexpected authorization scheme (expected Bearer or Negotiate)");
  return {Status(StatusCode::UNAUTHENTICATED, "Unexpected authorization scheme"), std::nullopt};
}

}  // namespace cta::admin_api::rpc
