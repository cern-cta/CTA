/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "WorkflowApiApp.hpp"

#include "CtaRpcWfeImpl.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/LogLevel.hpp"
#include "grpc-server/auth/jwt/JwtAuthFactory.hpp"
#include "grpc-server/server/ServerBuilderHelper.hpp"
#include "scheduler/batching/FrontendBatchingLayer.hpp"
#include "service-resources/ServiceResources.hpp"

namespace cta::workflow_api {

void WorkflowApiApp::stop() {
  {
    std::lock_guard lock(m_mutex);
    m_stopRequested = true;
  }
  m_stopCv.notify_all();
}

int WorkflowApiApp::run(const WorkflowApiConfig& config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);
  lc.log(cta::log::INFO, "Starting cta-workflow-api");

  // Catalogue, Scheduler DB
  service_resources::ServiceResources serviceResources(
    "Frontend",
    config.catalogue,
    config.scheduler,
    service_resources::makeConnectionOptions(config.catalogue, config.scheduler),
    log);

#ifdef CTA_PGSCHED
  // Opportunistic batching of archive/retrieve queueing, in front of the Scheduler
  const auto& batching = config.opportunistic_batching;
  const bool batchingEnabled = config.experimental.opportunistic_batching_enabled;
  FrontendBatchingLayer batchingLayer(serviceResources.getScheduler(),
                                      batchingEnabled,
                                      batching.window_ms,
                                      batching.max_batch_size);
  {
    cta::log::ScopedParamContainer params(lc);
    params.add("enabled", batchingEnabled)
      .add("windowMs", batching.window_ms)
      .add("maxBatchSize", batching.max_batch_size);
    lc.log(cta::log::INFO, "Opportunistic batching configured");
  }
#endif

  // JWT authentication: the JWKS cache is refreshed in the background by the manager itself
  auto jwtAuthManager = grpc_server::auth::makeJwtAuthManager(config.auth.jwt, log);

  // mTLS: the clients present a certificate which is checked against the configured aliases
  const bool useMtls = config.auth.mtls.has_value() && config.auth.mtls->enabled;
  if (useMtls) {
    lc.log(cta::log::INFO, "Using mutual TLS to authenticate WFE client requests");
  }

  // Create the gRPC service
  CtaRpcWfeImpl wfeService(serviceResources,
                           config,
#ifdef CTA_PGSCHED
                           batchingLayer,
#endif
                           jwtAuthManager);

  // Build the gRPC server and register the service
  grpc_server::ServerBuilderHelper serverBuilderHelper(config.grpc, useMtls, nullptr, lc);
  auto& serverBuilder = serverBuilderHelper.builder();
  serverBuilder.RegisterService(&wfeService);

  // Start the gRPC server and block waiting for a stop signal
  auto server = serverBuilderHelper.buildAndStart();
  lc.log(cta::log::INFO, "Listening on socket address: " + serverBuilderHelper.serverAddress());

  {
    std::unique_lock lock(m_mutex);
    m_stopCv.wait(lock, [this] { return m_stopRequested; });
  }

  lc.log(cta::log::INFO, "Stopping cta-workflow-api");
  server->Shutdown();
  server->Wait();
  // server->Wait() returns only after every in-flight RPC handler has exited: stop the batchers now so their
  // destructors run with no threads inside enqueueAndWait()
#ifdef CTA_PGSCHED
  batchingLayer.stop();
#endif
  return 0;
}

}  // namespace cta::workflow_api
