/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "grpc-server/auth/Authentication.hpp"
#include "grpc-server/auth/jwt/JwtAuthManager.hpp"
#include "rpc/auth/kerberos/TokenStore.hpp"

#include <functional>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace cta::admin_api::rpc {

/**
 * @brief Authenticates the admin API requests, accepting JWT ("Bearer") or Kerberos ("Negotiate") tokens
 *
 * Built once at startup and shared by the admin services. Each of the two methods can be disabled by passing an
 * empty optional for it, in which case requests using it are rejected.
 * Safe to be copied at will, since the contents are basically only references to existing structures
 */
class RequestAuthenticator {
public:
  /**
   * @param jwtAuthManager  JWT validation, nullptr if JWT auth is disabled
   * @param krbTokenStore   Kerberos token store, empty if Kerberos auth is disabled
   * @param ourInstance     Instance name to use for the SecurityIdentity
   */
  RequestAuthenticator(std::shared_ptr<cta::grpc_server::auth::JwtAuthManager> jwtAuthManager,
                       std::optional<std::reference_wrapper<kerberos::TokenStore>> krbTokenStore,
                       std::string ourInstance);

  /**
   * @brief Authenticates a request from its "authorization" header
   *
   * @param clientMetadata The gRPC client metadata containing the authorization header
   * @param clientHost     The peer connection string
   * @param lc             Log context for logging
   * @return OK with the client identity, or UNAUTHENTICATED if the header is missing, uses an unsupported scheme or
   *         fails validation
   */
  std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
  authenticate(const grpc_server::auth::GrpcMetadata& clientMetadata,
               const std::string& clientHost,
               cta::log::LogContext& lc) const;

private:
  std::shared_ptr<cta::grpc_server::auth::JwtAuthManager>
    m_jwtAuthManager;  //!< Shared JWK cache, nullptr if JWT is disabled
  std::optional<std::reference_wrapper<kerberos::TokenStore>> m_krbTokenStore;  //!< empty if Kerberos is disabled
  std::string m_ourInstance;  //!< Instance name used for the SecurityIdentity
};

}  // namespace cta::admin_api::rpc
