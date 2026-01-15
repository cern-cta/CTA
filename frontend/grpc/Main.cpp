/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2021 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FrontendGrpcService.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/JwkCache.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "rdbms/Login.hpp"
#include "version.h"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include "NegotiationService.hpp"
#include "TokenStorage.hpp"
#include "callback_api/CtaAdminServer.hpp"
#include "common/utils/utils.hpp"

#include <future>
#include <getopt.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <thread>

using namespace cta;
using namespace cta::common;
using namespace cta::frontend::grpc;

constexpr std::string_view defaultPort = "17017";

std::string help = "Usage: cta-frontend-grpc [options]\n"
                   "\n"
                   "where options can be:\n"
                   "\n"
                   "\t--config <config-file>, -c   \tConfiguration file\n"
                   "\t--version, -v                \tprint version and exit\n"
                   "\t--help, -h                   \tprint this help and exit\n";

static struct option long_options[] = {
  {"config",  required_argument, nullptr, 'c'},
  {"help",    no_argument,       nullptr, 'h'},
  {"version", no_argument,       nullptr, 'v'},
  {nullptr,   0,                 nullptr, 0  }
};

void printHelpAndExit(int rc) {
  std::cout << help << std::endl;
  exit(rc);
}

void printVersionAndExit() {
  std::cout << "cta-frontend-grpc version: " << CTA_VERSION << std::endl;
  exit(0);
}

void JwksCacheRefreshLoop(std::weak_ptr<JwkCache> weakCache,
                          std::future<void> shouldStopThread,
                          int cacheRefreshInterval,
                          const log::LogContext& lc) {
  log::LogContext threadLc(lc);
  threadLc.log(log::INFO, "Detached JWKS cache refresh thread started");

  while (shouldStopThread.wait_for(std::chrono::seconds(cacheRefreshInterval)) == std::future_status::timeout) {
    auto cache = weakCache.lock();
    if (!cache) {
      threadLc.log(log::INFO, "JwkCache no longer exists, exiting JWKS cache refresh thread");
      break;  // Cache destroyed
    }
    time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    threadLc.log(log::INFO, "Updating the JWKS cache");
    cache->updateCache(now);
  }
  threadLc.log(log::INFO, "Detached JWKS cache refresh thread ended");
}

int main(const int argc, char* const* const argv) {
  std::string config_file("/etc/cta/cta-frontend-grpc.conf");

  char c;
  int option_index = 0;
  const std::string shortHostName = utils::getShortHostname();
  bool useTLS = false;
  std::string port;

  while ((c = getopt_long(argc, argv, "c:hv", long_options, &option_index)) != EOF) {
    switch (c) {
      case 'h':
        printHelpAndExit(0);
        break;
      case 'v':
        printVersionAndExit();
        break;
      case 'c':
        config_file = optarg;
        break;
      default:
        printHelpAndExit(1);
    }
  }

  // Initialize frontend service first to get configuration
  auto frontendService = std::make_shared<cta::frontend::FrontendService>(config_file);

  // get the log context
  log::LogContext lc = frontendService->getLogContext();

  // Build the shared JWK cache here even if JWT is disabled, in this case it will never be populated
  CurlJwksFetcher jwksFetcher(frontendService->getJwksTotalTimeout().value_or(60));
  std::shared_ptr<JwkCache> jwkCache = std::make_shared<JwkCache>(jwksFetcher,
                                                                  frontendService->getJwksUri().value_or(""),
                                                                  frontendService->getPubkeyTimeout().value_or(0),
                                                                  frontendService->getLogContext());
  // Setup TokenStorage for Kerberos authentication
  cta::frontend::grpc::server::TokenStorage tokenStorage;

  // Initialize RPC service with shared frontend service and cache
  frontend::grpc::CtaRpcImpl svc(frontendService, jwkCache, tokenStorage);
  std::weak_ptr<JwkCache> weakCache = jwkCache;
  std::promise<void> shouldStopThreadPromise;
  std::future<void> shouldStopThreadFuture = shouldStopThreadPromise.get_future();
  std::thread cacheRefreshThread;
  // if token authentication is specified, then also start the refresh thread, otherwise no point in doing this
  if (frontendService->getJwtAuth()) {
    lc.log(log::INFO, "Starting the cache refresh thread for JWKS cache");
    cacheRefreshThread = std::thread(JwksCacheRefreshLoop,
                                     weakCache,
                                     std::move(shouldStopThreadFuture),
                                     frontendService->getCacheRefreshInterval().value_or(600),
                                     std::cref(lc));
  }

  // use castor config to avoid dependency on xroot-ssi
  // Configuration config(config_file);

  lc.log(log::INFO, "Starting cta-frontend-grpc- " + std::string(CTA_VERSION));

  // try to update port from config
  if (frontendService->getPort().has_value()) {
    port = frontendService->getPort().value();
  } else {
    port = defaultPort;
  }

  std::string server_address("0.0.0.0:" + port);

  // start gRPC service

  ServerBuilder builder;

  std::shared_ptr<grpc::ServerCredentials> creds;

  // read TLS value from config
  useTLS = frontendService->getTls();

  // get number of threads
  int threads = frontendService->getThreads().value_or(8 * std::thread::hardware_concurrency());

  if (useTLS) {
    lc.log(log::INFO, "Using gRPC over TLS");
    grpc::SslServerCredentialsOptions tls_options(GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE);
    grpc::SslServerCredentialsOptions::PemKeyCertPair cert;

    if (!frontendService->getTlsKey().has_value()) {
      throw exception::UserError("TLS specified but TLS key is not defined");
    } else if (!frontendService->getTlsCert().has_value()) {
      throw exception::UserError("TLS specified but TLS cert is not defined.");
    } else {
      auto key_file = frontendService->getTlsKey().value();
      lc.log(log::INFO, "TLS service key file: " + key_file);
      cert.private_key = cta::utils::file2string(key_file);

      auto cert_file = frontendService->getTlsCert().value();
      lc.log(log::INFO, "TLS service certificate file: " + cert_file);
      cert.cert_chain = cta::utils::file2string(cert_file);

      if (auto ca_chain = frontendService->getTlsChain(); ca_chain.has_value()) {
        lc.log(log::INFO, "TLS CA chain file: " + ca_chain.value());
        tls_options.pem_root_certs = cta::utils::file2string(ca_chain.value());
      } else {
        lc.log(log::INFO, "TLS CA chain file not defined ...");
        tls_options.pem_root_certs = "";
      }
      tls_options.pem_key_cert_pairs.emplace_back(std::move(cert));

      creds = grpc::SslServerCredentials(tls_options);
    }
  } else {
    lc.log(log::INFO, "Using gRPC over plaintext socket");
    creds = grpc::InsecureServerCredentials();
  }

  // enable health checking, needed by CI
  grpc::EnableDefaultHealthCheckService(true);
  // add reflection
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, creds);

  // fixed the number of request threads
  ResourceQuota quota;
  quota.SetMaxThreads(threads);
  lc.log(log::INFO, "Using " + std::to_string(threads) + " request processing threads");
  builder.SetResourceQuota(quota);

  // Get Kerberos configuration
  std::string strKeytab = frontendService->getKeytab().value_or("/etc/cta/cta-frontend.keytab");
  std::string strService = frontendService->getServicePrincipal().value_or("cta/" + shortHostName);

  lc.log(log::INFO, "Using keytab: " + strKeytab);
  lc.log(log::INFO, "Using service principal: " + strService);

  // Create completion queue that will be used only for the Kerberos negotiation service
  std::unique_ptr<::grpc::ServerCompletionQueue> negCq = builder.AddCompletionQueue();

  // Create negotiation service
  auto negotiationService = std::make_unique<cta::frontend::grpc::server::NegotiationService>(lc,
                                                                                              tokenStorage,
                                                                                              std::move(negCq),
                                                                                              strKeytab,
                                                                                              strService,
                                                                                              1 /* threads */);

  // Register negotiation service on main builder
  builder.RegisterService(&negotiationService->getService());

  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&svc);

  frontend::grpc::CtaRpcStreamImpl streamSvc(frontendService->getCatalogue(),
                                             frontendService->getScheduler(),
                                             frontendService->getSchedDb(),
                                             frontendService->getInstanceName(),
                                             frontendService->getCatalogueConnString(),
                                             frontendService->getMissingFileCopiesMinAgeSecs(),
                                             frontendService->getenableCtaAdminCommands(),
                                             frontendService->getLogContext(),
                                             frontendService->getJwtAuth(),
                                             jwkCache,
                                             tokenStorage);
  builder.RegisterService(&streamSvc);

  // add reflection
  // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // Build and start server
  std::unique_ptr<Server> server(builder.BuildAndStart());

  // Start negotiation service processing (after server is built)
  negotiationService->startProcessing();

  lc.log(cta::log::INFO, "Listening on socket address: " + server_address);
  server->Wait();

  // if we ever receive a shutdown, or want to handle termination of the frontend gracefully,
  // add the following line:
  shouldStopThreadPromise.set_value();
  if (cacheRefreshThread.joinable()) {
    cacheRefreshThread.join();
  }
}
