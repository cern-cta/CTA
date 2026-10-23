/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AdminApiApp.hpp"

#include "common/log/LogContext.hpp"
#include "common/log/LogLevel.hpp"
#include "grpc-server/auth/jwt/JwtAuthFactory.hpp"
#include "grpc-server/server/ServerBuilderHelper.hpp"
#include "rpc/CtaRpcStreamImpl.hpp"
#include "rpc/CtaRpcSyncImpl.hpp"
#include "rpc/auth/RequestAuthenticator.hpp"
#include "rpc/auth/kerberos/NegotiationService.hpp"
#include "rpc/auth/kerberos/TokenStore.hpp"
#include "service-resources/ServiceResources.hpp"

#include <functional>
#include <optional>

namespace cta::admin_api {

void AdminApiApp::stop() {
  {
    std::lock_guard lock(m_mutex);
    m_stopRequested = true;
  }
  m_stopCv.notify_all();
}

int AdminApiApp::run(const AdminApiConfig& config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);
  lc.log(cta::log::INFO, "Starting cta-admin-api");

  // Catalogue, Scheduler DB and Scheduler
  service_resources::ServiceResources serviceResources(config.catalogue, config.scheduler, log);

  // JWT authentication: the JWKS cache is refreshed in the background by the manager itself
  auto jwtAuthManager = grpc_server::auth::makeJwtAuthManager(config.auth.jwt, log);

  const bool kerberosEnabled = config.auth.kerberos.has_value() && config.auth.kerberos->enabled;

  // Kerberos Token Store - used if Kerberos is enabled
  rpc::kerberos::TokenStore krbTokenStore;
  std::optional<std::reference_wrapper<rpc::kerberos::TokenStore>> krbTokenStoreRef;
  if (kerberosEnabled) {
    // this reference will last through the lifetime of krbTokenStore, which will end
    // when the server is destroyed, at the end of the method
    krbTokenStoreRef = krbTokenStore;
  }

  // The authenticator will need access both to the JWT Auth Manager and the token store
  const rpc::RequestAuthenticator authenticator(jwtAuthManager, krbTokenStoreRef, config.instance_name);

  // Create both RPC endpoints (sync and stream commands)
  rpc::CtaRpcSyncImpl syncService(serviceResources, config, authenticator);
  rpc::CtaRpcStreamImpl streamService(serviceResources, config, authenticator);

  // Admin API clients are never authenticated with mTLS, hence `requireClientCertificate = false`
  grpc_server::ServerBuilderHelper serverBuilderHelper(config.grpc, false, nullptr, lc);
  auto& builder = serverBuilderHelper.builder();

  // Kerberos negotiation runs on its own completion queue
  // it is declared before the server so that it outlives it
  std::unique_ptr<rpc::kerberos::NegotiationService> krbNegotiationService;
  if (kerberosEnabled) {
    const auto& kerberos = *config.auth.kerberos;
    lc.log(cta::log::INFO,
           "Using Kerberos authentication with keytab '" + kerberos.keytab_path + "' and service principal '"
             + kerberos.service_principal + "'");
    krbNegotiationService = std::make_unique<rpc::kerberos::NegotiationService>(log,
                                                                                krbTokenStore,
                                                                                builder.AddCompletionQueue(),
                                                                                kerberos.keytab_path,
                                                                                kerberos.service_principal,
                                                                                1 /* threads */);
    // This service will be used by the client to obtain a token
    builder.RegisterService(&krbNegotiationService->getService());
  }

  // Regular endpoints, for sync and streaming services
  builder.RegisterService(&syncService);
  builder.RegisterService(&streamService);

  // Start everything up
  auto server = serverBuilderHelper.buildAndStart();
  if (krbNegotiationService) {
    krbNegotiationService->startProcessing();
  }
  lc.log(cta::log::INFO, "Listening on socket address: " + serverBuilderHelper.serverAddress());

  // wait until `.stop()` is called
  {
    std::unique_lock lock(m_mutex);
    m_stopCv.wait(lock, [this] { return m_stopRequested; });
  }

  lc.log(cta::log::INFO, "Stopping cta-admin-api");
  server->Shutdown();
  server->Wait();
  return 0;
}

}  // namespace cta::admin_api
